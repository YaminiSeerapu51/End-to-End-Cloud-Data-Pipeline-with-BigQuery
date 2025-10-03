{{ config(
    materialized='table',
    description='Marketing performance dimension table with campaign and channel analysis'
) }}

with campaign_data as (
    select * from {{ source('external_sources', 'ext_bigquery_campaigns') }}
),

channel_data as (
    select * from {{ source('external_sources', 'ext_bigquery_attribution') }}
),

daily_marketing as (
    select * from {{ ref('stg_bigquery_marketing') }}
),

campaign_summary as (
    select 
        campaign_id,
        campaign_name,
        campaign_type,
        total_spend,
        total_impressions,
        total_clicks,
        total_conversions,
        attributed_revenue,
        campaign_duration_days,
        overall_ctr,
        overall_conversion_rate,
        overall_cpc,
        overall_cpa,
        roas,
        performance_tier,
        efficiency_score,
        
        -- Performance rankings
        row_number() over (order by roas desc) as roas_rank,
        row_number() over (order by attributed_revenue desc) as revenue_rank,
        row_number() over (order by efficiency_score desc) as efficiency_rank,
        
        -- Performance percentiles
        percent_rank() over (order by roas) as roas_percentile,
        percent_rank() over (order by efficiency_score) as efficiency_percentile,
        
        -- Campaign health indicators
        case 
            when roas >= {{ var('excellent_roas_threshold') }} and efficiency_score >= 80 then 'Excellent'
            when roas >= {{ var('good_roas_threshold') }} and efficiency_score >= 60 then 'Good'
            when roas >= {{ var('breakeven_roas_threshold') }} and efficiency_score >= 40 then 'Average'
            else 'Needs Improvement'
        end as campaign_health_status,
        
        case 
            when total_spend > 0 and attributed_revenue > 0 
            then (attributed_revenue - total_spend) / total_spend * 100
            else 0
        end as roi_percentage
        
    from campaign_data
),

channel_summary as (
    select 
        channel,
        attribution_model,
        total_attributed_conversions,
        total_attributed_revenue,
        first_touch_conversions,
        last_touch_conversions,
        assisted_conversions,
        avg_time_to_conversion_days,
        channel_contribution_score,
        attribution_weight_total,
        
        -- Channel rankings
        row_number() over (order by total_attributed_revenue desc) as channel_revenue_rank,
        row_number() over (order by channel_contribution_score desc) as channel_contribution_rank,
        
        -- Channel efficiency metrics
        case 
            when total_attributed_conversions > 0 
            then total_attributed_revenue / total_attributed_conversions
            else 0
        end as revenue_per_conversion,
        
        case 
            when channel_contribution_score >= 20 then 'Primary'
            when channel_contribution_score >= 10 then 'Secondary'
            when channel_contribution_score >= 5 then 'Supporting'
            else 'Minor'
        end as channel_importance_tier
        
    from channel_data
),

marketing_trends as (
    select 
        extract(month from date_key) as month_num,
        extract(quarter from date_key) as quarter_num,
        extract(year from date_key) as year_num,
        
        sum(total_spend) as monthly_spend,
        avg(roas) as avg_monthly_roas,
        sum(total_conversions) as monthly_conversions,
        
        -- Trend calculations
        lag(sum(total_spend)) over (order by extract(year from date_key), extract(month from date_key)) as prev_month_spend,
        
        case 
            when lag(sum(total_spend)) over (order by extract(year from date_key), extract(month from date_key)) > 0
            then (sum(total_spend) - lag(sum(total_spend)) over (order by extract(year from date_key), extract(month from date_key))) 
                 / lag(sum(total_spend)) over (order by extract(year from date_key), extract(month from date_key)) * 100
            else 0
        end as spend_growth_pct
        
    from daily_marketing
    group by extract(month from date_key), extract(quarter from date_key), extract(year from date_key)
)

select 
    -- Campaign metrics
    cs.campaign_id,
    cs.campaign_name,
    cs.campaign_type,
    cs.total_spend as campaign_spend,
    cs.attributed_revenue as campaign_revenue,
    cs.roas as campaign_roas,
    cs.performance_tier as campaign_tier,
    cs.campaign_health_status,
    cs.roas_rank,
    cs.revenue_rank,
    cs.efficiency_rank,
    cs.roi_percentage as campaign_roi_pct,
    
    -- Channel metrics (using first channel for simplicity)
    (select channel from channel_summary order by channel_revenue_rank limit 1) as top_channel,
    (select channel_contribution_score from channel_summary order by channel_revenue_rank limit 1) as top_channel_contribution,
    (select channel_importance_tier from channel_summary order by channel_revenue_rank limit 1) as top_channel_tier,
    
    -- Aggregated metrics
    (select sum(monthly_spend) from marketing_trends) as total_annual_spend,
    (select avg(avg_monthly_roas) from marketing_trends) as avg_annual_roas,
    (select avg(spend_growth_pct) from marketing_trends where spend_growth_pct is not null) as avg_monthly_growth_pct,
    
    current_timestamp() as created_at
    
from campaign_summary cs
cross join (select 1 as dummy) -- Cross join to include aggregated metrics for all campaigns
