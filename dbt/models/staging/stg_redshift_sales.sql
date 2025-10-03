{{ config(
    materialized='view',
    description='Staging model for Redshift sales data - daily sales summary'
) }}

with source_data as (
    select 
        date_key,
        total_orders,
        total_revenue,
        total_items_sold,
        unique_customers,
        avg_order_value,
        avg_items_per_order,
        top_product_category,
        top_state,
        delivered_orders,
        delivery_performance_score
    from {{ source('external_sources', 'ext_redshift_daily_sales') }}
),

cleaned_data as (
    select 
        date_key,
        coalesce(total_orders, 0) as total_orders,
        coalesce(total_revenue, 0) as total_revenue,
        coalesce(total_items_sold, 0) as total_items_sold,
        coalesce(unique_customers, 0) as unique_customers,
        coalesce(avg_order_value, 0) as avg_order_value,
        coalesce(avg_items_per_order, 0) as avg_items_per_order,
        coalesce(top_product_category, 'Unknown') as top_product_category,
        coalesce(top_state, 'Unknown') as top_state,
        coalesce(delivered_orders, 0) as delivered_orders,
        coalesce(delivery_performance_score, 0) as delivery_performance_score,
        
        -- Data quality flags
        case when total_revenue > 0 then true else false end as has_revenue_data,
        case when unique_customers > 0 then true else false end as has_customer_data,
        
        -- Business metrics
        case 
            when total_orders > 0 then total_revenue / total_orders 
            else 0 
        end as calculated_aov,
        
        case 
            when delivered_orders > 0 and total_orders > 0 
            then (delivered_orders::float / total_orders) * 100 
            else 0 
        end as delivery_rate_pct
        
    from source_data
    where date_key is not null
      and date_key between '{{ var("start_date") }}' and '{{ var("end_date") }}'
)

select * from cleaned_data
