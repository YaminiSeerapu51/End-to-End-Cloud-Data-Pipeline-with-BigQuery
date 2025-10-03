{{ config(
    materialized='table',
    description='User engagement dimension table with comprehensive user metrics'
) }}

with user_profiles as (
    select * from {{ source('staging', 'user_profiles') }}
),

user_engagement_metrics as (
    select * from {{ source('processed', 'user_engagement_metrics') }}
),

feature_engagement as (
    select * from {{ source('processed', 'feature_engagement_summary') }}
),

daily_engagement as (
    select * from {{ ref('stg_snowflake_engagement') }}
),

user_summary as (
    select 
        up.user_id,
        up.customer_id,
        up.user_segment,
        up.subscription_tier,
        up.user_lifecycle_stage,
        up.engagement_score as profile_engagement_score,
        up.total_sessions as lifetime_sessions,
        up.total_session_duration_minutes as lifetime_session_minutes,
        up.total_page_views as lifetime_page_views,
        up.total_feature_interactions as lifetime_feature_interactions,
        up.last_login_date,
        up.days_since_last_login,
        up.signup_date,
        
        -- Enhanced metrics from user_engagement_metrics
        coalesce(uem.recent_sessions, 0) as recent_sessions_30d,
        coalesce(uem.recent_session_minutes, 0) as recent_session_minutes_30d,
        coalesce(uem.features_used, 0) as features_used_30d,
        uem.last_session_timestamp,
        
        -- Calculated engagement metrics
        case 
            when up.total_sessions > 0 
            then up.total_session_duration_minutes::float / up.total_sessions
            else 0
        end as avg_session_duration_lifetime,
        
        case 
            when up.total_sessions > 0 
            then up.total_page_views::float / up.total_sessions
            else 0
        end as avg_page_views_per_session,
        
        case 
            when up.total_sessions > 0 
            then up.total_feature_interactions::float / up.total_sessions
            else 0
        end as avg_interactions_per_session,
        
        -- Engagement trends
        case 
            when coalesce(uem.recent_sessions, 0) > 0 and up.total_sessions > 0
            then (coalesce(uem.recent_sessions, 0) * 30.0) / up.total_sessions * 
                 datediff(day, up.signup_date, current_date())
            else 0
        end as engagement_trend_score,
        
        -- User value scoring
        case 
            when up.engagement_score >= {{ var('high_engagement_score') }} then 'High Value'
            when up.engagement_score >= {{ var('medium_engagement_score') }} then 'Medium Value'
            when up.engagement_score >= 20 then 'Low Value'
            else 'At Risk'
        end as user_value_tier,
        
        -- Lifecycle scoring
        case 
            when up.user_lifecycle_stage = 'Active' and up.engagement_score >= {{ var('high_engagement_score') }} then 'Champion'
            when up.user_lifecycle_stage = 'Active' and up.engagement_score >= {{ var('medium_engagement_score') }} then 'Loyal'
            when up.user_lifecycle_stage = 'New' and up.engagement_score >= {{ var('medium_engagement_score') }} then 'Potential'
            when up.user_lifecycle_stage = 'At Risk' then 'At Risk'
            when up.user_lifecycle_stage = 'Churned' then 'Lost'
            else 'Casual'
        end as user_persona,
        
        -- Subscription value
        case up.subscription_tier
            when 'Premium' then 100
            when 'Pro' then 50
            when 'Free' then 10
            else 0
        end as subscription_value_score
        
    from user_profiles up
    left join user_engagement_metrics uem on up.user_id = uem.user_id
),

engagement_aggregates as (
    select 
        count(*) as total_users,
        count(case when user_lifecycle_stage = 'Active' then 1 end) as active_users,
        count(case when user_lifecycle_stage = 'At Risk' then 1 end) as at_risk_users,
        count(case when user_lifecycle_stage = 'Churned' then 1 end) as churned_users,
        count(case when user_value_tier = 'High Value' then 1 end) as high_value_users,
        
        avg(profile_engagement_score) as avg_engagement_score,
        avg(lifetime_sessions) as avg_lifetime_sessions,
        avg(avg_session_duration_lifetime) as avg_session_duration,
        
        -- Percentile calculations
        percentile_cont(0.25) within group (order by profile_engagement_score) as engagement_score_p25,
        percentile_cont(0.50) within group (order by profile_engagement_score) as engagement_score_p50,
        percentile_cont(0.75) within group (order by profile_engagement_score) as engagement_score_p75,
        percentile_cont(0.90) within group (order by profile_engagement_score) as engagement_score_p90
        
    from user_summary
),

feature_popularity as (
    select 
        feature_name,
        feature_category,
        total_interactions,
        unique_users,
        interactions_per_user,
        row_number() over (order by total_interactions desc) as popularity_rank,
        row_number() over (order by interactions_per_user desc) as engagement_rank
    from feature_engagement
),

top_features as (
    select 
        listagg(feature_name, ', ') within group (order by popularity_rank) as top_features_by_volume,
        listagg(case when engagement_rank <= 5 then feature_name end, ', ') within group (order by engagement_rank) as top_features_by_engagement
    from feature_popularity
    where popularity_rank <= 10
)

select 
    us.user_id,
    us.customer_id,
    us.user_segment,
    us.subscription_tier,
    us.user_lifecycle_stage,
    us.user_persona,
    us.user_value_tier,
    us.profile_engagement_score,
    us.lifetime_sessions,
    us.lifetime_session_minutes,
    us.recent_sessions_30d,
    us.recent_session_minutes_30d,
    us.features_used_30d,
    us.avg_session_duration_lifetime,
    us.avg_page_views_per_session,
    us.avg_interactions_per_session,
    us.engagement_trend_score,
    us.subscription_value_score,
    us.last_login_date,
    us.days_since_last_login,
    us.signup_date,
    
    -- Engagement percentile ranking
    case 
        when us.profile_engagement_score >= ea.engagement_score_p90 then 'Top 10%'
        when us.profile_engagement_score >= ea.engagement_score_p75 then 'Top 25%'
        when us.profile_engagement_score >= ea.engagement_score_p50 then 'Top 50%'
        else 'Bottom 50%'
    end as engagement_percentile,
    
    -- Risk scoring
    case 
        when us.days_since_last_login > 30 then 'High Risk'
        when us.days_since_last_login > 14 then 'Medium Risk'
        when us.days_since_last_login > 7 then 'Low Risk'
        else 'Active'
    end as churn_risk_level,
    
    -- Feature adoption score
    case 
        when us.features_used_30d >= 10 then 'Power User'
        when us.features_used_30d >= 5 then 'Regular User'
        when us.features_used_30d >= 2 then 'Light User'
        else 'Minimal User'
    end as feature_adoption_level,
    
    -- Aggregated insights
    ea.total_users as total_platform_users,
    ea.avg_engagement_score as platform_avg_engagement,
    tf.top_features_by_volume,
    tf.top_features_by_engagement,
    
    current_timestamp() as created_at
    
from user_summary us
cross join engagement_aggregates ea
cross join top_features tf
