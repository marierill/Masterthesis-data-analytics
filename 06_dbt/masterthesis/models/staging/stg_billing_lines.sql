-- models/staging/stg_billing_lines.sql
-- Staging layer: clean pass-through from raw fact table.
-- No business logic here – only column selection and type alignment.
-- Materialized as view (zero storage cost, always current).

with source as (
    select * from fact_billing_lines
),

staged as (
    select
        billing_line_id,
        invoice_id,
        date_key,
        customer_key,
        product_key,
        region_key,
        costcenter_key,
        subscription_type,
        billing_period_start,
        billing_period_end,
        quantity,
        unit_price,
        discount_amount,
        revenue,
        cost,
        -- derived margin (calculated here once, available to all downstream models)
        round(revenue - cost, 2) as margin
    from source
)

select * from staged