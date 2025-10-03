{{ config(
    materialized='view',
    description='Staging model for Snowflake product engagement data'
) }}

with source_data as (
    select 
        date_key,
        active_users,
        total_sessions,
        total_session_minutes,
        avg_session_duration,
        total_page_views,
        avg_page_views_per_session,
        bounce_sessions,
        conversion_sessions,
        total_session_value,
        bounce_rate,
        conversion_rate
    from {{ source('processed', 'daily_product_engagement') }}
),

cleaned_data as (
    select 
        date_key,
        coalesce(active_users, 0) as active_users,
        coalesce(total_sessions, 0) as total_sessions,
        coalesce(total_session_minutes, 0) as total_session_minutes,
        coalesce(avg_session_duration, 0) as avg_session_duration,
        coalesce(total_page_views, 0) as total_page_views,
        coalesce(avg_page_views_per_session, 0) as avg_page_views_per_session,
        coalesce(bounce_sessions, 0) as bounce_sessions,
        coalesce(conversion_sessions, 0) as conversion_sessions,
        coalesce(total_session_value, 0) as total_session_value,
        coalesce(bounce_rate, 0) as bounce_rate,
        coalesce(conversion_rate, 0) as conversion_rate,
        
        -- Data quality flags
        case when active_users > 0 then true else false end as has_user_data,
        case when total_sessions > 0 then true else false end as has_session_data,
        case when total_session_value > 0 then true else false end as has_value_data,
        
        -- Engagement quality metrics
        case 
            when total_sessions > 0 then active_users::float / total_sessions
            else 0
        end as users_per_session_ratio,
        
        case 
            when active_users > 0 then total_page_views::float / active_users
            else 0
        end as page_views_per_user,
        
        case 
            when conversion_sessions > 0 then total_session_value / conversion_sessions
            else 0
        end as avg_conversion_value,
        
        -- Engagement score calculation
        case 
            when avg_session_duration >= 10 and bounce_rate <= 30 and conversion_rate >= 5 then 'High'
            when avg_session_duration >= 5 and bounce_rate <= 50 and conversion_rate >= 2 then 'Medium'
            when avg_session_duration >= 2 and bounce_rate <= 70 then 'Low'
            else 'Very Low'
        end as engagement_quality,
        
        -- Calculate engagement score (0-100)
        least(100, 
            (case when avg_session_duration > 0 then least(avg_session_duration * 2, 20) else 0 end) +
            (case when bounce_rate < 100 then (100 - bounce_rate) * 0.3 else 0 end) +
            (case when conversion_rate > 0 then least(conversion_rate * 5, 25) else 0 end) +
            (case when avg_page_views_per_session > 1 then least((avg_page_views_per_session - 1) * 5, 15) else 0 end) +
            (case when active_users > 100 then 20 when active_users > 50 then 15 when active_users > 10 then 10 else 5 end)
        ) as engagement_score
        
    from source_data
    where date_key is not null
      and date_key between '{{ var("start_date") }}' and '{{ var("end_date") }}'
)

select * from cleaned_data
