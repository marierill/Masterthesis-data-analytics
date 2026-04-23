-- models/marts/mart_revenue_breakdown.sql
-- Mart layer: Revenue breakdown by customer segment and plan tier.
-- Joins fact with dim_customer and dim_product.
-- Materialized as table (physically stored – Power BI reads from here).
-- Logically equivalent to Q6 + Q7 in kpi_queries.sql (benchmark reference).

with billing as (
    select * from {{ ref('stg_billing_lines') }}
),

customers as (
    select
        customer_key,
        customer_segment,
        acquisition_channel,
        contract_type,
        industry
    from dim_customer
),

products as (
    select
        product_key,
        plan_name,
        plan_tier,
        product_category,
        pricing_model
    from dim_product
),

enriched as (
    select
        b.billing_line_id,
        b.date_key,
        b.revenue,
        b.cost,
        b.margin,
        b.subscription_type,
        c.customer_segment,
        c.acquisition_channel,
        c.contract_type,
        p.plan_tier,
        p.pricing_model
    from billing b
    join customers c on b.customer_key = c.customer_key
    join products p  on b.product_key  = p.product_key
),

aggregated as (
    select
        customer_segment,
        plan_tier,
        pricing_model,
        subscription_type,
        count(*)                                                as line_count,
        sum(revenue)                                            as total_revenue,
        sum(cost)                                               as total_cost,
        sum(margin)                                             as total_margin,
        round(sum(margin) / nullif(sum(revenue), 0), 4)        as margin_ratio,
        round(sum(revenue) / count(*), 2)                      as avg_revenue_per_line
    from enriched
    group by
        customer_segment,
        plan_tier,
        pricing_model,
        subscription_type
)

select * from aggregated
order by total_revenue desc