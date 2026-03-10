from __future__ import annotations

import pytest


@pytest.mark.contract
def test_kpi_definitions_available():
    """
    Contract: KPI SQL definitions should exist in the repo.
    Suggested: 03 embedded dwh/kpi_queries.sql and 04 cloud dwh/kpi_queries.sql
    """
    pytest.skip("Enable once KPI SQL files exist.")


@pytest.mark.contract
def test_margin_identity_holds():
    """
    Contract: SUM(revenue - cost) equals SUM(revenue) - SUM(cost) for any dataset.
    """
    pytest.skip("Enable once data + DuckDB KPI queries are implemented.")
