-- models/marts/mart_mrr_monthly.sql
-- Mart layer: Monthly Recurring Revenue (MRR) per year/month.
-- Filters to recurring subscription_type only (84% of billing lines).
-- Joins with dim_date to extract year/month.
-- Materialized as table (physically stored – Power BI reads from here).
-- Logically equivalent to Q4 in kpi_queries.sql (benchmark reference).

with billing as (
    select * from {{ ref('stg_billing_lines') }}
),

dates as (
    select
        date_key,
        year,
        month
    from dim_date
),

mrr_base as (
    select
        d.year,
        d.month,
        sum(b.revenue)                                          as mrr,
        count(distinct b.customer_key)                          as active_customers,
        sum(b.revenue) / count(distinct b.customer_key)         as arpa,
        sum(b.margin)                                           as total_margin,
        round(sum(b.margin) / nullif(sum(b.revenue), 0), 4)    as margin_ratio
    from billing b
    join dates d
        on b.date_key = d.date_key
    where b.subscription_type = 'recurring'
    group by d.year, d.month
),

with_growth as (
    select
        year,
        month,
        mrr,
        active_customers,
        arpa,
        total_margin,
        margin_ratio,
        lag(mrr) over (order by year, month)                    as prev_month_mrr,
        round(
            (mrr - lag(mrr) over (order by year, month))
            / nullif(lag(mrr) over (order by year, month), 0),
            4
        )                                                       as mom_growth
    from mrr_base
)

select * from with_growth
order by year, month