-- =============================================================
-- DuckDB Star Schema DDL
-- Architecture B: Local Embedded Analytics (DuckDB)
-- Based on: 01_scope/architecture_overview.md
-- =============================================================

-- Drop tables if they exist (for re-runs)
DROP TABLE IF EXISTS fact_billing_lines;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_costcenter;

-- Dimension: Date
CREATE TABLE dim_date (
    date_key        INTEGER     PRIMARY KEY,
    full_date       DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    month           TINYINT     NOT NULL,
    quarter         TINYINT     NOT NULL,
    week            TINYINT     NOT NULL,
    day_of_week     TINYINT     NOT NULL
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key         INTEGER     PRIMARY KEY,
    plan_name           VARCHAR     NOT NULL,
    plan_tier           VARCHAR     NOT NULL,  -- Basic / Pro / Enterprise
    product_category    VARCHAR     NOT NULL,  -- Core / Add-on / Support / Automation
    pricing_model       VARCHAR     NOT NULL   -- flat / usage / hybrid
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key        INTEGER     PRIMARY KEY,
    customer_segment    VARCHAR     NOT NULL,  -- SMB / Mid-Market / Enterprise
    acquisition_channel VARCHAR     NOT NULL,  -- inbound / outbound / partner / self_service
    contract_type       VARCHAR     NOT NULL,  -- monthly / annual / multi_year
    industry            VARCHAR     NOT NULL
);

-- Dimension: Region
CREATE TABLE dim_region (
    region_key      INTEGER     PRIMARY KEY,
    country         VARCHAR     NOT NULL,
    region          VARCHAR     NOT NULL,  -- EMEA / AMER
    sales_area      VARCHAR     NOT NULL
);

-- Dimension: Cost Center
CREATE TABLE dim_costcenter (
    costcenter_key  INTEGER     PRIMARY KEY,
    department      VARCHAR     NOT NULL,
    cost_type       VARCHAR     NOT NULL
);

-- Fact Table: Billing Lines
CREATE TABLE fact_billing_lines (
    billing_line_id         BIGINT      PRIMARY KEY,
    invoice_id              BIGINT      NOT NULL,
    date_key                INTEGER     NOT NULL REFERENCES dim_date(date_key),
    customer_key            INTEGER     NOT NULL REFERENCES dim_customer(customer_key),
    product_key             INTEGER     NOT NULL REFERENCES dim_product(product_key),
    region_key              INTEGER     NOT NULL REFERENCES dim_region(region_key),
    costcenter_key          INTEGER     NOT NULL REFERENCES dim_costcenter(costcenter_key),
    subscription_type       VARCHAR     NOT NULL,  -- recurring / usage / one_time
    billing_period_start    DATE        NOT NULL,
    billing_period_end      DATE        NOT NULL,
    quantity                SMALLINT    NOT NULL,
    unit_price              DECIMAL(10,2) NOT NULL,
    discount_amount         DECIMAL(10,2) NOT NULL,
    revenue                 DECIMAL(10,2) NOT NULL,
    cost                    DECIMAL(10,2) NOT NULL
);
