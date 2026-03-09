-- =============================================================
-- Snowflake KPI Queries
-- Architecture A: Cloud Data Warehouse (Snowflake)
-- Based on: 01_scope/kpi_catalog.md
-- IMPORTANT: Logical query structure is identical to
--            03_embedded_dwh/kpi_queries.sql. Only syntax may differ.
-- =============================================================

-- ---------------------------------------------------------------
-- Q1: Total Revenue
-- Category: Simple Aggregation
-- ---------------------------------------------------------------
SELECT
    SUM(revenue) AS total_revenue
FROM fact_billing_lines;


-- ---------------------------------------------------------------
-- Q2: Total Cost
-- Category: Simple Aggregation
-- ---------------------------------------------------------------
SELECT
    SUM(cost) AS total_cost
FROM fact_billing_lines;


-- ---------------------------------------------------------------
-- Q3: Contribution Margin + Margin Ratio
-- Category: Simple Aggregation
-- ---------------------------------------------------------------
SELECT
    SUM(revenue - cost)                         AS contribution_margin,
    SUM(revenue - cost) / NULLIF(SUM(revenue), 0) AS margin_ratio
FROM fact_billing_lines;


-- ---------------------------------------------------------------
-- Q4: Monthly Recurring Revenue (MRR)
-- Category: Filtered Aggregation
-- ---------------------------------------------------------------
SELECT
    d.year,
    d.month,
    SUM(f.revenue) AS mrr
FROM fact_billing_lines f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.subscription_type = 'recurring'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- ---------------------------------------------------------------
-- Q5: Average Revenue per Account (ARPA)
-- Category: Filtered Aggregation
-- ---------------------------------------------------------------
SELECT
    d.year,
    d.month,
    SUM(f.revenue) / COUNT(DISTINCT f.customer_key) AS arpa
FROM fact_billing_lines f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.subscription_type = 'recurring'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- ---------------------------------------------------------------
-- Q6: Revenue by Plan Tier
-- Category: Multi-dimensional GROUP BY
-- ---------------------------------------------------------------
SELECT
    p.plan_tier,
    SUM(f.revenue)                                    AS revenue_by_tier,
    SUM(f.revenue) / SUM(SUM(f.revenue)) OVER ()     AS tier_share
FROM fact_billing_lines f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.plan_tier
ORDER BY revenue_by_tier DESC;


-- ---------------------------------------------------------------
-- Q7: Revenue by Customer Segment
-- Category: Multi-dimensional GROUP BY
-- ---------------------------------------------------------------
SELECT
    c.customer_segment,
    SUM(f.revenue) AS revenue_by_segment
FROM fact_billing_lines f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment
ORDER BY revenue_by_segment DESC;


-- ---------------------------------------------------------------
-- Q8: Revenue Growth Month-over-Month (MoM)
-- Category: Window Function
-- ---------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        SUM(f.revenue) AS monthly_rev
    FROM fact_billing_lines f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    monthly_rev,
    LAG(monthly_rev) OVER (ORDER BY year, month)                     AS prev_month_rev,
    (monthly_rev - LAG(monthly_rev) OVER (ORDER BY year, month))
        / NULLIF(LAG(monthly_rev) OVER (ORDER BY year, month), 0)    AS mom_growth
FROM monthly_revenue
ORDER BY year, month;


-- ---------------------------------------------------------------
-- Q9: Revenue Growth Year-over-Year (YoY)
-- Category: Window Function
-- ---------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        SUM(f.revenue) AS monthly_rev
    FROM fact_billing_lines f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    monthly_rev,
    LAG(monthly_rev, 12) OVER (ORDER BY year, month)                       AS same_month_prev_year,
    (monthly_rev - LAG(monthly_rev, 12) OVER (ORDER BY year, month))
        / NULLIF(LAG(monthly_rev, 12) OVER (ORDER BY year, month), 0)      AS yoy_growth
FROM monthly_revenue
ORDER BY year, month;


-- ---------------------------------------------------------------
-- Q10: Rolling 3-Month Revenue Average
-- Category: Window Function
-- ---------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        SUM(f.revenue) AS monthly_rev
    FROM fact_billing_lines f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    monthly_rev,
    AVG(monthly_rev) OVER (
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3m_avg
FROM monthly_revenue
ORDER BY year, month;


-- ---------------------------------------------------------------
-- Q11: Cumulative Revenue Year-to-Date (YTD)
-- Category: Window Function
-- ---------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        SUM(f.revenue) AS monthly_rev
    FROM fact_billing_lines f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    monthly_rev,
    SUM(monthly_rev) OVER (
        PARTITION BY year
        ORDER BY month
    ) AS revenue_ytd
FROM monthly_revenue
ORDER BY year, month;


-- ---------------------------------------------------------------
-- Q12: Revenue Concentration - Top-10% Customer Share
-- Category: Ranking / Window Function
-- ---------------------------------------------------------------
WITH customer_revenue AS (
    SELECT
        customer_key,
        SUM(revenue) AS total_customer_rev
    FROM fact_billing_lines
    GROUP BY customer_key
),
ranked AS (
    SELECT
        customer_key,
        total_customer_rev,
        NTILE(10) OVER (ORDER BY total_customer_rev DESC) AS decile
    FROM customer_revenue
)
SELECT
    SUM(CASE WHEN decile = 1 THEN total_customer_rev ELSE 0 END)
        / NULLIF(SUM(total_customer_rev), 0)  AS top10pct_revenue_share
FROM ranked;


-- ---------------------------------------------------------------
-- Q13: Monthly Aggregated Revenue (Input for Anomaly Detection)
-- Category: Time-Series Aggregation
-- ---------------------------------------------------------------
SELECT
    d.year,
    d.month,
    SUM(f.revenue) AS monthly_revenue
FROM fact_billing_lines f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
