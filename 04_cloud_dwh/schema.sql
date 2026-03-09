-- =============================================================
-- Snowflake Star Schema DDL
-- Architecture A: Cloud Data Warehouse (Snowflake)
-- Based on: 01_scope/architecture_overview.md
-- IMPORTANT: Logical schema is identical to 03_embedded_dwh/schema.sql.
--            Only platform syntax differs.
-- =============================================================

-- Run within the correct DATABASE and SCHEMA context:
-- USE DATABASE <your_db>;
-- USE SCHEMA benchmark;

-- Drop tables if they exist (for re-runs)
DROP TABLE IF EXISTS fact_billing_lines;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_costcenter;

-- Dimension: Date
CREATE TABLE dim_date (
    date_key        INTEGER     NOT NULL PRIMARY KEY,
    full_date       DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    month           TINYINT     NOT NULL,
    quarter         TINYINT     NOT NULL,
    week            TINYINT     NOT NULL,
    day_of_week     TINYINT     NOT NULL
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key         INTEGER     NOT NULL PRIMARY KEY,
    plan_name           VARCHAR     NOT NULL,
    plan_tier           VARCHAR     NOT NULL,
    product_category    VARCHAR     NOT NULL,
    pricing_model       VARCHAR     NOT NULL
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key        INTEGER     NOT NULL PRIMARY KEY,
    customer_segment    VARCHAR     NOT NULL,
    acquisition_channel VARCHAR     NOT NULL,
    contract_type       VARCHAR     NOT NULL,
    industry            VARCHAR     NOT NULL
);

-- Dimension: Region
CREATE TABLE dim_region (
    region_key      INTEGER     NOT NULL PRIMARY KEY,
    country         VARCHAR     NOT NULL,
    region          VARCHAR     NOT NULL,
    sales_area      VARCHAR     NOT NULL
);

-- Dimension: Cost Center
CREATE TABLE dim_costcenter (
    costcenter_key  INTEGER     NOT NULL PRIMARY KEY,
    department      VARCHAR     NOT NULL,
    cost_type       VARCHAR     NOT NULL
);

-- Fact Table: Billing Lines
CREATE TABLE fact_billing_lines (
    billing_line_id         NUMBER(18,0)    NOT NULL PRIMARY KEY,
    invoice_id              NUMBER(18,0)    NOT NULL,
    date_key                INTEGER         NOT NULL,
    customer_key            INTEGER         NOT NULL,
    product_key             INTEGER         NOT NULL,
    region_key              INTEGER         NOT NULL,
    costcenter_key          INTEGER         NOT NULL,
    subscription_type       VARCHAR         NOT NULL,
    billing_period_start    DATE            NOT NULL,
    billing_period_end      DATE            NOT NULL,
    quantity                SMALLINT        NOT NULL,
    unit_price              NUMBER(10,2)    NOT NULL,
    discount_amount         NUMBER(10,2)    NOT NULL,
    revenue                 NUMBER(10,2)    NOT NULL,
    cost                    NUMBER(10,2)    NOT NULL
);
-- Note: Snowflake enforces PK/FK as informational constraints only (not enforced at insert).
-- Foreign key constraints are documented in architecture_overview.md.
