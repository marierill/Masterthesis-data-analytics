from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from hashlib import md5
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


@dataclass(frozen=True)
class VolumeSpec:
    label: str
    rows: int


VOLUME_OFFSETS = {"small": 11, "medium": 17, "large": 23}

# Rows per chunk for Parquet writing (reduces peak RAM for large volume)
CHUNK_SIZE = 2_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic BI benchmark data.")
    parser.add_argument(
        "--config",
        default="02_data_generation/config.yaml",
        help="Path to generator config YAML.",
    )
    parser.add_argument(
        "--volume",
        choices=["small", "medium", "large"],
        default="small",
        help="Configured volume profile.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Optional explicit row count override.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory override.",
    )
    return parser.parse_args()


def load_config(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def weighted_choice(rng: np.random.Generator, mapping: dict[str, float], size: int) -> np.ndarray:
    labels = list(mapping.keys())
    weights = np.array(list(mapping.values()), dtype=float)
    weights /= weights.sum()
    return rng.choice(labels, size=size, p=weights)


def build_dim_date(start: str, end: str) -> pd.DataFrame:
    full_dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"full_date": full_dates})
    iso = df["full_date"].dt.isocalendar()
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["month"] = df["full_date"].dt.month
    df["quarter"] = df["full_date"].dt.quarter
    df["week"] = iso["week"].astype(int)
    df["day_of_week"] = df["full_date"].dt.dayofweek + 1
    return df[
        ["date_key", "full_date", "year", "month", "quarter", "week", "day_of_week"]
    ].sort_values("date_key")


def build_dim_product(cfg: dict, rng: np.random.Generator) -> pd.DataFrame:
    n_products = int(cfg["dimensions"]["products"])
    tiers = weighted_choice(rng, cfg["weights"]["plan_tier"], n_products)
    models = weighted_choice(rng, cfg["weights"]["pricing_model"], n_products)
    categories = rng.choice(["Core", "Add-on", "Support", "Automation"], size=n_products)
    return pd.DataFrame(
        {
            "product_key": np.arange(1, n_products + 1),
            "plan_name": [f"Plan-{i:03d}" for i in range(1, n_products + 1)],
            "plan_tier": tiers,
            "product_category": categories,
            "pricing_model": models,
        }
    )


def build_dim_customer(cfg: dict, rng: np.random.Generator) -> pd.DataFrame:
    n_customers = int(cfg["dimensions"]["customers"])
    return pd.DataFrame(
        {
            "customer_key": np.arange(1, n_customers + 1),
            "customer_segment": weighted_choice(
                rng, cfg["weights"]["customer_segment"], n_customers
            ),
            "acquisition_channel": weighted_choice(
                rng, cfg["weights"]["acquisition_channel"], n_customers
            ),
            "contract_type": weighted_choice(rng, cfg["weights"]["contract_type"], n_customers),
            "industry": rng.choice(
                ["SaaS", "Retail", "Manufacturing", "Health", "Finance", "Education"],
                size=n_customers,
            ),
        }
    )


def build_dim_region(cfg: dict, rng: np.random.Generator) -> pd.DataFrame:
    n_regions = int(cfg["dimensions"]["regions"])
    countries = np.array(
        [
            "Germany", "France", "Spain", "Italy", "Netherlands",
            "Sweden", "Poland", "Austria", "Belgium", "Ireland",
            "USA", "Canada", "Brazil", "Japan", "Singapore",
        ]
    )
    selected = rng.choice(countries, size=n_regions, replace=False)
    return pd.DataFrame(
        {
            "region_key": np.arange(1, n_regions + 1),
            "country": selected,
            "region": ["EMEA" if c not in {"USA", "Canada", "Brazil"} else "AMER" for c in selected],
            "sales_area": [f"Area-{i:02d}" for i in range(1, n_regions + 1)],
        }
    )


def build_dim_costcenter(cfg: dict, rng: np.random.Generator) -> pd.DataFrame:
    n_costcenters = int(cfg["dimensions"]["costcenters"])
    departments = rng.choice(
        ["Engineering", "Sales", "Marketing", "Support", "Operations", "G&A"],
        size=n_costcenters,
        replace=True,
    )
    return pd.DataFrame(
        {
            "costcenter_key": np.arange(1, n_costcenters + 1),
            "department": departments,
            "cost_type": rng.choice(
                ["Infrastructure", "Personnel", "Licenses", "Acquisition"], size=n_costcenters
            ),
        }
    )


def _build_unit_price_array(
    cfg: dict, dim_product: pd.DataFrame, rng: np.random.Generator
) -> np.ndarray:
    price_ranges = cfg["pricing"]["unit_price_by_tier"]
    prices = np.empty(dim_product.shape[0], dtype=float)
    for idx, tier in enumerate(dim_product["plan_tier"].to_numpy()):
        low, high = price_ranges[tier]
        prices[idx] = rng.uniform(low, high)
    return prices


def _build_fact_chunk(
    cfg: dict,
    chunk_start_id: int,
    chunk_size: int,
    date_keys: np.ndarray,
    date_values: np.ndarray,
    product_unit_prices: np.ndarray,
    n_customers: int,
    n_products: int,
    n_regions: int,
    n_costcenters: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Build one chunk of fact_billing_lines rows."""
    n = chunk_size

    product_keys = rng.integers(1, n_products + 1, size=n, endpoint=False)
    sampled_dates_idx = rng.integers(0, len(date_keys), size=n, endpoint=False)
    sampled_dates = date_values[sampled_dates_idx]

    quantity = np.clip(rng.poisson(lam=2.2, size=n) + 1, 1, 20)
    base_unit_price = product_unit_prices[product_keys - 1]
    price_noise = rng.normal(loc=1.0, scale=0.08, size=n)
    unit_price = np.maximum(base_unit_price * price_noise, 1.0)

    gross_revenue = quantity * unit_price

    discount_prob = float(cfg["pricing"]["discount_probability"])
    discount_max = float(cfg["pricing"]["discount_max_share"])
    has_discount = rng.random(n) < discount_prob
    discount_rate = rng.beta(a=2.0, b=6.0, size=n) * discount_max
    discount_amount = np.where(has_discount, gross_revenue * discount_rate, 0.0)

    revenue = np.maximum(gross_revenue - discount_amount, 0.5)
    cost_low, cost_high = cfg["pricing"]["cost_share_range"]
    cost_share = rng.uniform(cost_low, cost_high, size=n)
    cost = revenue * cost_share

    if cfg["anomalies"]["enabled"]:
        anomaly_rate = float(cfg["anomalies"]["rate"])
        n_anomalies = int(n * anomaly_rate)
        if n_anomalies > 0:
            anom_idx = rng.choice(n, size=n_anomalies, replace=False)
            mult_low, mult_high = cfg["anomalies"]["multiplier_range"]
            multipliers = rng.uniform(mult_low, mult_high, size=n_anomalies)
            revenue[anom_idx] *= multipliers
            cost[anom_idx] = revenue[anom_idx] * np.minimum(
                cost_share[anom_idx] + 0.05, 0.95
            )

    period_start = pd.to_datetime(sampled_dates).to_period("M").to_timestamp()
    period_end = pd.to_datetime(period_start) + pd.offsets.MonthEnd(0)

    ids = np.arange(chunk_start_id, chunk_start_id + n)

    return pd.DataFrame(
        {
            "billing_line_id": ids,
            "invoice_id": np.floor((ids - 1) / 2).astype(int) + 1,
            "date_key": date_keys[sampled_dates_idx],
            "customer_key": rng.integers(1, n_customers + 1, size=n, endpoint=False),
            "product_key": product_keys,
            "region_key": rng.integers(1, n_regions + 1, size=n, endpoint=False),
            "costcenter_key": rng.integers(1, n_costcenters + 1, size=n, endpoint=False),
            "subscription_type": weighted_choice(rng, cfg["weights"]["subscription_type"], n),
            "billing_period_start": period_start,
            "billing_period_end": period_end,
            "quantity": quantity.astype(int),
            "unit_price": unit_price.round(2),
            "discount_amount": discount_amount.round(2),
            "revenue": revenue.round(2),
            "cost": cost.round(2),
        }
    )


def write_table(df: pd.DataFrame, table_name: str, out_dir: Path, cfg: dict) -> None:
    formats = cfg["output"]["formats"]
    if "parquet" in formats:
        df.to_parquet(
            out_dir / f"{table_name}.parquet",
            index=False,
            compression=cfg["output"]["parquet_compression"],
        )
    if "csv" in formats:
        df.to_csv(out_dir / f"{table_name}.csv", index=False)


def write_fact_chunked(
    cfg: dict,
    volume: VolumeSpec,
    dim_date: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_region: pd.DataFrame,
    dim_costcenter: pd.DataFrame,
    out_dir: Path,
    rng: np.random.Generator,
) -> None:
    """
    Write fact_billing_lines in chunks to keep peak RAM low.
    For small/medium volumes a single chunk suffices.
    For large (20M rows) uses multiple chunks of CHUNK_SIZE rows.
    """
    n_total = volume.rows
    formats = cfg["output"]["formats"]

    date_keys = dim_date["date_key"].to_numpy()
    date_values = dim_date["full_date"].to_numpy(dtype="datetime64[ns]")
    product_unit_prices = _build_unit_price_array(cfg, dim_product, rng)

    n_customers = len(dim_customer)
    n_products = len(dim_product)
    n_regions = len(dim_region)
    n_costcenters = len(dim_costcenter)

    parquet_writer = None
    csv_chunks = []

    row_id = 1
    rows_remaining = n_total
    chunk_idx = 0

    while rows_remaining > 0:
        this_chunk = min(CHUNK_SIZE, rows_remaining)
        chunk_idx += 1
        print(
            f"  fact_billing_lines: chunk {chunk_idx} "
            f"({row_id:,} – {row_id + this_chunk - 1:,} of {n_total:,})"
        )

        chunk_df = _build_fact_chunk(
            cfg=cfg,
            chunk_start_id=row_id,
            chunk_size=this_chunk,
            date_keys=date_keys,
            date_values=date_values,
            product_unit_prices=product_unit_prices,
            n_customers=n_customers,
            n_products=n_products,
            n_regions=n_regions,
            n_costcenters=n_costcenters,
            rng=rng,
        )

        if "parquet" in formats:
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    out_dir / "fact_billing_lines.parquet",
                    table.schema,
                    compression=cfg["output"]["parquet_compression"],
                )
            parquet_writer.write_table(table)

        if "csv" in formats:
            csv_chunks.append(chunk_df)

        row_id += this_chunk
        rows_remaining -= this_chunk

        # Free chunk memory immediately
        del chunk_df

    if parquet_writer is not None:
        parquet_writer.close()

    if "csv" in formats and csv_chunks:
        print("  fact_billing_lines: writing CSV...")
        pd.concat(csv_chunks, ignore_index=True).to_csv(
            out_dir / "fact_billing_lines.csv", index=False
        )
        del csv_chunks


def write_metadata(
    cfg: dict, volume: VolumeSpec, out_dir: Path, dim_tables: dict[str, pd.DataFrame]
) -> None:
    config_hash = md5(
        json.dumps(cfg, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    now = pd.Timestamp.utcnow().isoformat()

    row_counts = {name: int(df.shape[0]) for name, df in dim_tables.items()}
    row_counts["fact_billing_lines"] = volume.rows

    metadata = {
        "created_at_utc": now,
        "seed": int(cfg["seed"]),
        "volume": volume.label,
        "rows_fact_billing_lines": volume.rows,
        "config_hash_md5": config_hash,
        "tables": row_counts,
        "date_range": cfg["date_range"],
    }

    with (out_dir / "metadata.json").open("w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2, default=str)


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    if args.volume not in cfg["volumes"]:
        raise ValueError(f"Unknown volume '{args.volume}' in config volumes.")

    rows = int(args.rows) if args.rows is not None else int(cfg["volumes"][args.volume])
    volume = VolumeSpec(label=args.volume, rows=rows)

    seed = int(cfg["seed"]) + VOLUME_OFFSETS[volume.label]
    rng = np.random.default_rng(seed)

    print(f"Building dimensions for volume='{volume.label}'...")
    dim_date = build_dim_date(cfg["date_range"]["start"], cfg["date_range"]["end"])
    dim_product = build_dim_product(cfg, rng)
    dim_customer = build_dim_customer(cfg, rng)
    dim_region = build_dim_region(cfg, rng)
    dim_costcenter = build_dim_costcenter(cfg, rng)

    root = Path(args.output_dir or cfg["output"]["root_dir"]) / volume.label
    root.mkdir(parents=True, exist_ok=True)

    dim_tables = {
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_region": dim_region,
        "dim_costcenter": dim_costcenter,
    }

    print("Writing dimension tables...")
    for name, table in dim_tables.items():
        write_table(table, name, root, cfg)

    print(f"Building and writing fact_billing_lines ({volume.rows:,} rows, chunk size {CHUNK_SIZE:,})...")
    write_fact_chunked(
        cfg=cfg,
        volume=volume,
        dim_date=dim_date,
        dim_product=dim_product,
        dim_customer=dim_customer,
        dim_region=dim_region,
        dim_costcenter=dim_costcenter,
        out_dir=root,
        rng=rng,
    )

    write_metadata(cfg, volume, root, dim_tables)
    print(f"Generated volume='{volume.label}' rows={volume.rows} at {root}")


if __name__ == "__main__":
    main()