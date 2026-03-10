# KPI Catalog

This document defines all KPIs used in the analytics platform comparison.

Each KPI includes:
- Business meaning
- Mathematical definition
- SQL interpretation
- Aggregation logic
- Drilldown capability

All KPI logic must remain identical across platforms.

---

# 1. Core Revenue KPIs

## 1.1 Total Revenue

**Business Meaning**  
Total billed revenue within a selected time period.

**Formula**
SUM(revenue)

**SQL Interpretation**
SELECT SUM(revenue)
FROM fact_billing_lines
WHERE date BETWEEN X AND Y;

**Drilldowns**
- by plan_tier
- by customer_segment
- by region
- by pricing_model

---

## 1.2 Total Cost

**Business Meaning**  
Allocated cost of delivered SaaS services.

**Formula**
SUM(cost)

---

## 1.3 Contribution Margin

**Business Meaning**  
Operational profitability before fixed overhead.

**Formula**
SUM(revenue - cost)

Alternative:
SUM(revenue) - SUM(cost)

---

## 1.4 Margin Ratio

**Business Meaning**  
Profitability percentage.

**Formula**
SUM(revenue - cost) / SUM(revenue)

---

# 2. SaaS-Specific KPIs

## 2.1 Monthly Recurring Revenue (MRR)

**Business Meaning**  
Normalized recurring revenue for a given month.

Only recurring subscription billing lines are included.

**Formula**
SUM(revenue)
WHERE subscription_type = 'recurring'
AND billing_period overlaps selected month

Aggregation Level:
Monthly

---

## 2.2 Average Revenue per Account (ARPA)

**Business Meaning**  
Average monthly revenue per active customer.

**Formula**
SUM(revenue) / COUNT(DISTINCT customer_key)

Filtered for recurring revenue per month.

---

## 2.3 Revenue Growth (Month-over-Month)

**Business Meaning**  
Relative change of revenue between consecutive months.

**Formula**
(Current Month Revenue - Previous Month Revenue)
/
Previous Month Revenue

SQL requires window functions.

---

## 2.4 Revenue Growth (Year-over-Year)

**Formula**
(Current Month Revenue - Revenue Same Month Previous Year)
/
Revenue Same Month Previous Year

---

## 2.5 Revenue by Plan Tier

**Business Meaning**  
Revenue distribution across subscription tiers.

**Formula**
SUM(revenue)
GROUP BY plan_tier

---

## 2.6 Revenue by Customer Segment

**Formula**
SUM(revenue)
GROUP BY customer_segment

---

# 3. Structural & Distribution KPIs

## 3.1 Revenue Concentration (Top-N Share)

**Business Meaning**  
Revenue share of top 10% customers.

**Formula**
Revenue of Top 10% Customers
/
Total Revenue

Requires ranking by revenue per customer.

---

## 3.2 Revenue Distribution Statistics

**Metrics**
- Mean revenue per billing line
- Median revenue
- Standard deviation

Used for anomaly modeling input validation.

---

# 4. Time-Based Analytical KPIs

## 4.1 Rolling 3-Month Revenue Average

**Formula**
AVG(monthly_revenue)
OVER (
    ORDER BY month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)

---

## 4.2 Cumulative Revenue (Year-to-Date)

**Formula**
SUM(revenue)
OVER (
    PARTITION BY year
    ORDER BY month
)

---

# 5. Anomaly Detection Input Metric

## 5.1 Monthly Aggregated Revenue

Input to Python anomaly detection model.

**Definition**
Monthly SUM(revenue)

This aggregated time series is exported to Python and analyzed using:

- Z-score detection
- Isolation Forest (optional)
- IQR-based outlier detection

The implementation must be identical for both architectures.

---

# 6. Aggregation Rules

- All KPIs must be calculable from fact_billing_lines.
- No additional fact tables.
- All derived metrics must be computed via SQL or Python.
- Window functions allowed.
- No pre-aggregated materializations unless mirrored on both platforms.

---

# 7. Benchmark-Relevant Workload Types

The KPI set intentionally includes:

- Simple aggregations
- Multi-dimensional groupings
- Window functions
- Ranking functions
- Time-series calculations

This ensures realistic BI workload simulation across volume levels.