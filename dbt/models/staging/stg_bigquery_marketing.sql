{{ config(
    materialized='view',
    description='Staging model for BigQuery marketing data - daily marketing summary'
) }}

with source_data as (
    select 
        date_key,
        total_spend,
        total_impressions,
        total_clicks,
        total_conversions,
        unique_campaigns,
        active_channels,
        avg_ctr,
        avg_conversion_rate,
        avg_cpc,
        avg_cpa,
        top_performing_channel,
        top_performing_campaign,
        channel_performance_score,
        roas
    from {{ source('external_sources', 'ext_bigquery_marketing_daily') }}
),

cleaned_data as (
    select 
        date_key,
        coalesce(total_spend, 0) as total_spend,
        coalesce(total_impressions, 0) as total_impressions,
        coalesce(total_clicks, 0) as total_clicks,
        coalesce(total_conversions, 0) as total_conversions,
        coalesce(unique_campaigns, 0) as unique_campaigns,
        coalesce(active_channels, 0) as active_channels,
        coalesce(avg_ctr, 0) as avg_ctr,
        coalesce(avg_conversion_rate, 0) as avg_conversion_rate,
        coalesce(avg_cpc, 0) as avg_cpc,
        coalesce(avg_cpa, 0) as avg_cpa,
        coalesce(top_performing_channel, 'Unknown') as top_performing_channel,
        coalesce(top_performing_campaign, 'Unknown') as top_performing_campaign,
        coalesce(channel_performance_score, 0) as channel_performance_score,
        coalesce(roas, 0) as roas,
        
        -- Data quality flags
        case when total_spend > 0 then true else false end as has_spend_data,
        case when total_impressions > 0 then true else false end as has_impression_data,
        case when total_conversions > 0 then true else false end as has_conversion_data,
        
        -- Performance tiers
        case 
            when roas >= {{ var('excellent_roas_threshold') }} then 'Excellent'
            when roas >= {{ var('good_roas_threshold') }} then 'Good'
            when roas >= {{ var('breakeven_roas_threshold') }} then 'Break-even'
            else 'Poor'
        end as roas_tier,
        
        -- Efficiency metrics
        case 
            when total_impressions > 0 then (total_clicks::float / total_impressions) * 100
            else 0
        end as calculated_ctr,
        
        case 
            when total_clicks > 0 then (total_conversions::float / total_clicks) * 100
            else 0
        end as calculated_conversion_rate,
        
        -- Cost efficiency
        case 
            when total_spend > 0 and total_conversions > 0 
            then total_spend / total_conversions
            else null
        end as calculated_cpa
        
    from source_data
    where date_key is not null
      and date_key between '{{ var("start_date") }}' and '{{ var("end_date") }}'
)

select * from cleaned_data
