{{ config(
    materialized='view',
    description='Intermediate model combining daily metrics from all sources'
) }}

with sales_data as (
    select * from {{ ref('stg_redshift_sales') }}
),

marketing_data as (
    select * from {{ ref('stg_bigquery_marketing') }}
),

engagement_data as (
    select * from {{ ref('stg_snowflake_engagement') }}
),

date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('start_date') ~ "' as date)",
        end_date="cast('" ~ var('end_date') ~ "' as date)"
    ) }}
),

unified_daily as (
    select 
        ds.date_day as date_key,
        
        -- Sales metrics
        coalesce(s.total_orders, 0) as total_orders,
        coalesce(s.total_revenue, 0) as total_revenue,
        coalesce(s.total_items_sold, 0) as total_items_sold,
        coalesce(s.unique_customers, 0) as unique_customers,
        coalesce(s.avg_order_value, 0) as avg_order_value,
        coalesce(s.delivery_rate_pct, 0) as delivery_rate_pct,
        s.has_revenue_data,
        
        -- Marketing metrics
        coalesce(m.total_spend, 0) as total_marketing_spend,
        coalesce(m.total_impressions, 0) as total_impressions,
        coalesce(m.total_clicks, 0) as total_clicks,
        coalesce(m.total_conversions, 0) as total_conversions,
        coalesce(m.roas, 0) as marketing_roas,
        coalesce(m.calculated_ctr, 0) as marketing_ctr,
        coalesce(m.calculated_conversion_rate, 0) as marketing_conversion_rate,
        m.roas_tier,
        m.has_spend_data,
        
        -- Engagement metrics
        coalesce(e.active_users, 0) as active_users,
        coalesce(e.total_sessions, 0) as total_sessions,
        coalesce(e.avg_session_duration, 0) as avg_session_duration,
        coalesce(e.total_page_views, 0) as total_page_views,
        coalesce(e.bounce_rate, 0) as bounce_rate,
        coalesce(e.conversion_rate, 0) as engagement_conversion_rate,
        coalesce(e.engagement_score, 0) as engagement_score,
        e.engagement_quality,
        e.has_user_data,
        
        -- Data availability flags
        case when s.date_key is not null then true else false end as sales_data_available,
        case when m.date_key is not null then true else false end as marketing_data_available,
        case when e.date_key is not null then true else false end as engagement_data_available,
        
        -- Calculate data completeness score
        (case when s.date_key is not null then 33.33 else 0 end +
         case when m.date_key is not null then 33.33 else 0 end +
         case when e.date_key is not null then 33.34 else 0 end) as data_completeness_score
        
    from date_spine ds
    left join sales_data s on ds.date_day = s.date_key
    left join marketing_data m on ds.date_day = m.date_key
    left join engagement_data e on ds.date_day = e.date_key
)

select * from unified_daily
