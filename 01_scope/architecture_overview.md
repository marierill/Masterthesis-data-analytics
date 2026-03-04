# Architecture Overview

## 1. Architectural Objective

This project compares two analytics platform architectures under identical SaaS BI workloads:

- **Architecture A:** Cloud Data Warehouse (managed, scalable compute-storage separation)
- **Architecture B:** DuckDB (local, embedded, columnar, file-based)

The comparison is strictly controlled:

- Identical synthetic dataset
- Identical Star Schema
- Identical KPI logic
- Identical SQL workloads
- Identical anomaly detection task
- Identical volume levels

The data model and workload definition are platform-agnostic.

---

## 2. SaaS Business Context

The architecture supports a **SaaS subscription performance analytics** scenario.

Typical analytical questions:

- How does recurring revenue evolve over time?
- Which plans and customer segments drive MRR?
- Where do expansion and contraction effects occur?
- Are there unusual revenue fluctuations?

The system must support:

- Standard KPI dashboards
- Multi-dimensional drilldowns
- Time-based comparisons (MoM / YoY)
- Aggregations at plan and segment level
- Python-based anomaly detection

---

## 3. Modeling Approach

### Star Schema (Reference Model)

- One central fact table
- Conformed dimensions
- Surrogate keys
- No snowflake extensions
- No additional fact tables

Rationale:

- Analytical performance focus
- Clear semantic model
- Controlled benchmarking conditions
- Avoidance of join-topology bias in platform comparison

---

## 4. Granularity

### Fact Table Grain

**One row = one billing line item.**

Each billing line represents a single charged subscription component within an invoice.

Examples:

- Monthly base subscription
- Add-on charge
- Usage-based component
- Discounted plan fee

This grain allows:

- MRR derivation
- Revenue aggregation
- Product-level analysis
- Customer-level analysis
- Time-based trend analysis

Granularity is fixed and identical across both architectures.

---

## 5. Data Model

### 5.1 Fact Table

**fact_billing_lines**

Columns:

- billing_line_id (PK)
- invoice_id
- date_key (FK -> dim_date)
- customer_key (FK -> dim_customer)
- product_key (FK -> dim_product)
- region_key (FK -> dim_region)
- costcenter_key (FK -> dim_costcenter)
- subscription_type
- billing_period_start
- billing_period_end
- quantity
- unit_price
- discount_amount
- revenue (final charged amount)
- cost (allocated cost)

Derived (calculated in SQL):
- margin = revenue - cost

---

### 5.2 Dimension Tables

#### dim_date
- date_key (PK)
- full_date
- year
- month
- quarter
- week
- day_of_week

#### dim_product
- product_key (PK)
- plan_name
- plan_tier
- product_category
- pricing_model (flat / usage / hybrid)

#### dim_customer
- customer_key (PK)
- customer_segment (SMB / Mid-Market / Enterprise)
- acquisition_channel
- contract_type
- industry

#### dim_region
- region_key (PK)
- country
- region
- sales_area

#### dim_costcenter
- costcenter_key (PK)
- department
- cost_type

---

## 6. Time Horizon

- 24 months historical data
- Daily calendar dimension
- No real-time streaming

---

## 7. Data Volume Levels

Three dataset sizes:

- 500,000 billing lines
- 5,000,000 billing lines
- 20,000,000 billing lines

All datasets follow identical statistical distributions and schema.

No additional volume levels will be introduced.

---

## 8. Workload Definition

### 8.1 Core SaaS KPI Queries

- Total Revenue
- Monthly Recurring Revenue (MRR)
- Contribution Margin
- Average Revenue per Account (ARPA)
- Revenue Growth (MoM / YoY)
- Revenue by Plan Tier
- Revenue by Customer Segment

### 8.2 Aggregation & Drilldowns

- Revenue by plan
- Revenue by region
- Revenue by segment
- Top-N customers
- Revenue distribution analysis

### 8.3 Time-Based Analytics

- Rolling 3-month revenue average
- Month-over-month growth
- Year-over-year comparison

### 8.4 Python-Based Anomaly Detection

- Aggregated monthly revenue
- Detection of statistical outliers
- Identical implementation for both platforms

---

## 9. Architectural Constraints

To ensure strict comparability:

- No logical query modifications between platforms.
- Only syntax/platform adjustments allowed.
- No platform-specific indexing strategies unless mirrored.
- No caching tricks unique to one system.
- No additional materialized views.

---

## 10. Non-Goals

Explicitly excluded:

- Real-time analytics
- Event streaming
- Data lake integrations
- Advanced ML beyond anomaly detection
- Additional architectures
- Additional fact tables

---

## 11. Evaluation Focus

The architecture comparison evaluates:

- Query performance
- Storage footprint
- Implementation effort
- Transformation effort
- Time-to-First-Insight
- Maintainability & governance characteristics

This document defines the structural baseline of the entire analytics project.

