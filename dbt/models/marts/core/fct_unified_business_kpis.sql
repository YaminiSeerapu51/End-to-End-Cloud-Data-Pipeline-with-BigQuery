{{ config(
    materialized='table',
    description='Final unified business KPIs fact table',
    post_hook="grant select on {{ this }} to role analyst_role"
) }}

with daily_metrics as (
    select * from {{ ref('int_daily_unified_metrics') }}
),

calculated_kpis as (
    select 
        date_key,
        
        -- Core business metrics
        total_orders,
        total_revenue,
        total_items_sold,
        unique_customers,
        avg_order_value,
        delivery_rate_pct,
        
        -- Marketing metrics
        total_marketing_spend,
        total_impressions,
        total_clicks,
        total_conversions,
        marketing_roas,
        marketing_ctr,
        marketing_conversion_rate,
        roas_tier,
        
        -- Engagement metrics
        active_users,
        total_sessions,
        avg_session_duration,
        total_page_views,
        bounce_rate,
        engagement_conversion_rate,
        engagement_score,
        engagement_quality,
        
        -- Advanced KPIs
        case 
            when unique_customers > 0 and total_marketing_spend > 0 
            then total_marketing_spend / unique_customers
            else 0
        end as customer_acquisition_cost,
        
        case 
            when active_users > 0 and total_revenue > 0
            then total_revenue / active_users
            else 0
        end as revenue_per_user,
        
        case 
            when total_marketing_spend > 0 and total_revenue > 0
            then (total_revenue - total_marketing_spend) / total_marketing_spend * 100
            else 0
        end as marketing_roi_percentage,
        
        -- Efficiency ratios
        case 
            when total_marketing_spend > 0 and total_revenue > 0
            then total_revenue / total_marketing_spend
            else 0
        end as marketing_efficiency_ratio,
        
        case 
            when total_sessions > 0 and total_revenue > 0
            then total_revenue / total_sessions
            else 0
        end as revenue_per_session,
        
        -- Composite scores
        least(100, 
            (case when total_revenue > 0 then least(total_revenue / 1000, 30) else 0 end) +
            (case when marketing_roas > 0 then least(marketing_roas * 5, 25) else 0 end) +
            (case when engagement_score > 0 then engagement_score * 0.25 else 0 end) +
            (case when delivery_rate_pct > 0 then delivery_rate_pct * 0.2 else 0 end)
        ) as overall_business_score,
        
        -- Customer value metrics
        avg_order_value * 3.5 as estimated_customer_lifetime_value, -- Simplified CLV
        
        case 
            when unique_customers > 0 and total_orders > 0
            then total_orders::float / unique_customers
            else 0
        end as orders_per_customer,
        
        -- Data quality indicators
        sales_data_available,
        marketing_data_available,
        engagement_data_available,
        data_completeness_score,
        
        -- Performance flags
        case when marketing_roas >= {{ var('excellent_roas_threshold') }} then true else false end as is_high_roas_day,
        case when engagement_score >= {{ var('high_engagement_score') }} then true else false end as is_high_engagement_day,
        case when total_revenue > 0 and total_marketing_spend > 0 and (total_revenue / total_marketing_spend) > 2 then true else false end as is_profitable_day,
        
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from daily_metrics
)

select * from calculated_kpis
