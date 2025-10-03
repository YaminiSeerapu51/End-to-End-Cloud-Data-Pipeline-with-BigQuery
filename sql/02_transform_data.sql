-- =====================================================
-- Google Play Store Reviews Analysis - Data Transformation
-- Phase 2: Clean, transform, and create analysis-ready table
-- =====================================================

-- =====================================================
-- CREATE ANALYSIS-READY TABLE
-- =====================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.play_store_analysis.reviews_analyzed` AS

WITH cleaned_reviews AS (
  SELECT 
    -- Core fields
    review_text,
    sentiment,
    rating,
    app_name,
    
    -- Review characteristics
    review_length,
    review_word_count,
    review_length_category,
    rating_category,
    sentiment_binary,
    
    -- Data quality flags
    CASE 
      WHEN review_text IS NOT NULL 
           AND LENGTH(TRIM(review_text)) >= 3 
           AND rating BETWEEN 1 AND 5 
           AND sentiment IN ('Positive', 'Negative', 'Neutral')
      THEN TRUE 
      ELSE FALSE 
    END as is_valid_review,
    
    -- Timestamp fields
    ingestion_timestamp,
    data_source,
    
    -- Row number for deduplication
    ROW_NUMBER() OVER (
      PARTITION BY review_text, rating, sentiment 
      ORDER BY ingestion_timestamp DESC
    ) as row_num
    
  FROM `your-gcp-project-id.play_store_analysis.raw_reviews`
  
  -- Basic filtering
  WHERE review_text IS NOT NULL
    AND LENGTH(TRIM(review_text)) >= 3
    AND rating BETWEEN 1 AND 5
    AND sentiment IN ('Positive', 'Negative', 'Neutral')
),

enhanced_features AS (
  SELECT 
    *,
    
    -- Advanced text features
    ARRAY_LENGTH(SPLIT(review_text, ' ')) as calculated_word_count,
    
    -- Sentiment scoring (more granular than binary)
    CASE sentiment
      WHEN 'Positive' THEN 1.0
      WHEN 'Neutral' THEN 0.5
      WHEN 'Negative' THEN 0.0
    END as sentiment_score,
    
    -- Review length buckets (more granular)
    CASE 
      WHEN review_length <= 25 THEN 'Very Short'
      WHEN review_length <= 75 THEN 'Short'
      WHEN review_length <= 200 THEN 'Medium'
      WHEN review_length <= 500 THEN 'Long'
      ELSE 'Very Long'
    END as detailed_length_category,
    
    -- Rating buckets for analysis
    CASE 
      WHEN rating <= 2 THEN 'Low (1-2)'
      WHEN rating = 3 THEN 'Medium (3)'
      WHEN rating >= 4 THEN 'High (4-5)'
    END as rating_group,
    
    -- Sentiment-Rating alignment check
    CASE 
      WHEN sentiment = 'Positive' AND rating >= 4 THEN 'Aligned'
      WHEN sentiment = 'Negative' AND rating <= 2 THEN 'Aligned'
      WHEN sentiment = 'Neutral' AND rating = 3 THEN 'Aligned'
      ELSE 'Misaligned'
    END as sentiment_rating_alignment,
    
    -- Statistical analysis flags
    CASE WHEN sentiment IN ('Positive', 'Negative') THEN TRUE ELSE FALSE END as include_in_ttest,
    
    -- Outlier detection for review length
    CASE 
      WHEN review_length > (
        SELECT APPROX_QUANTILES(review_length, 100)[OFFSET(95)] 
        FROM `your-gcp-project-id.play_store_analysis.raw_reviews`
      ) THEN TRUE 
      ELSE FALSE 
    END as is_length_outlier,
    
    -- Review quality indicators
    CASE 
      WHEN review_length >= 50 AND review_word_count >= 10 THEN 'Detailed'
      WHEN review_length >= 20 AND review_word_count >= 5 THEN 'Moderate'
      ELSE 'Brief'
    END as review_quality,
    
    -- Engagement indicators (proxy metrics)
    CASE 
      WHEN review_length > 100 THEN 'High Engagement'
      WHEN review_length > 50 THEN 'Medium Engagement'
      ELSE 'Low Engagement'
    END as engagement_level
    
  FROM cleaned_reviews
  WHERE row_num = 1  -- Remove duplicates
    AND is_valid_review = TRUE
),

final_dataset AS (
  SELECT 
    -- Generate unique ID for each review
    GENERATE_UUID() as review_id,
    
    -- Core analysis fields
    review_text,
    sentiment,
    rating,
    app_name,
    
    -- Length and word count features
    review_length,
    COALESCE(calculated_word_count, review_word_count) as word_count,
    detailed_length_category as length_category,
    review_quality,
    engagement_level,
    
    -- Rating and sentiment features
    rating_group,
    sentiment_score,
    sentiment_binary,
    sentiment_rating_alignment,
    
    -- Analysis flags
    include_in_ttest,
    is_length_outlier,
    
    -- Metadata
    ingestion_timestamp,
    data_source,
    CURRENT_TIMESTAMP() as transformation_timestamp,
    
    -- Derived metrics for analysis
    CASE WHEN sentiment = 'Positive' THEN rating ELSE NULL END as positive_rating,
    CASE WHEN sentiment = 'Negative' THEN rating ELSE NULL END as negative_rating,
    CASE WHEN sentiment = 'Neutral' THEN rating ELSE NULL END as neutral_rating
    
  FROM enhanced_features
)

SELECT * FROM final_dataset;

-- =====================================================
-- CREATE SUMMARY STATISTICS TABLE
-- =====================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.play_store_analysis.summary_statistics` AS

WITH basic_stats AS (
  SELECT 
    COUNT(*) as total_reviews,
    COUNT(DISTINCT app_name) as unique_apps,
    AVG(rating) as avg_rating,
    STDDEV(rating) as stddev_rating,
    AVG(review_length) as avg_review_length,
    STDDEV(review_length) as stddev_review_length,
    AVG(word_count) as avg_word_count,
    MIN(rating) as min_rating,
    MAX(rating) as max_rating,
    MIN(review_length) as min_review_length,
    MAX(review_length) as max_review_length
  FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
),

sentiment_stats AS (
  SELECT 
    sentiment,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    AVG(rating) as avg_rating_by_sentiment,
    AVG(review_length) as avg_length_by_sentiment,
    STDDEV(rating) as stddev_rating_by_sentiment
  FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
  GROUP BY sentiment
),

length_category_stats AS (
  SELECT 
    length_category,
    COUNT(*) as count,
    AVG(rating) as avg_rating_by_length,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
  FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
  GROUP BY length_category
),

rating_distribution AS (
  SELECT 
    rating,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
  FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
  GROUP BY rating
  ORDER BY rating
)

SELECT 
  'Dataset Overview' as metric_category,
  CAST(total_reviews AS STRING) as metric_value,
  'Total number of reviews analyzed' as description
FROM basic_stats

UNION ALL

SELECT 
  'Dataset Overview' as metric_category,
  CAST(ROUND(avg_rating, 2) AS STRING) as metric_value,
  'Average rating across all reviews' as description
FROM basic_stats

UNION ALL

SELECT 
  'Dataset Overview' as metric_category,
  CAST(ROUND(avg_review_length, 0) AS STRING) as metric_value,
  'Average review length (characters)' as description
FROM basic_stats

UNION ALL

SELECT 
  'Sentiment Distribution' as metric_category,
  CONCAT(sentiment, ': ', CAST(count AS STRING), ' (', CAST(percentage AS STRING), '%)') as metric_value,
  CONCAT('Average rating: ', CAST(ROUND(avg_rating_by_sentiment, 2) AS STRING)) as description
FROM sentiment_stats

UNION ALL

SELECT 
  'Length Category Distribution' as metric_category,
  CONCAT(length_category, ': ', CAST(count AS STRING), ' (', CAST(percentage AS STRING), '%)') as metric_value,
  CONCAT('Average rating: ', CAST(ROUND(avg_rating_by_length, 2) AS STRING)) as description
FROM length_category_stats

UNION ALL

SELECT 
  'Rating Distribution' as metric_category,
  CONCAT('Rating ', CAST(rating AS STRING), ': ', CAST(count AS STRING), ' (', CAST(percentage AS STRING), '%)') as metric_value,
  'Distribution of star ratings' as description
FROM rating_distribution;

-- =====================================================
-- DATA VALIDATION QUERIES
-- =====================================================

-- Validate transformation results
SELECT 
  'Transformation Validation' as check_type,
  COUNT(*) as total_records,
  COUNT(CASE WHEN sentiment IN ('Positive', 'Negative', 'Neutral') THEN 1 END) as valid_sentiment,
  COUNT(CASE WHEN rating BETWEEN 1 AND 5 THEN 1 END) as valid_rating,
  COUNT(CASE WHEN review_length > 0 THEN 1 END) as valid_length,
  COUNT(CASE WHEN include_in_ttest = TRUE THEN 1 END) as records_for_ttest
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`;

-- Check for potential data issues
SELECT 
  'Data Quality Check' as check_type,
  COUNT(CASE WHEN sentiment_rating_alignment = 'Misaligned' THEN 1 END) as misaligned_records,
  COUNT(CASE WHEN is_length_outlier = TRUE THEN 1 END) as length_outliers,
  COUNT(CASE WHEN word_count = 0 THEN 1 END) as zero_word_count,
  ROUND(AVG(CASE WHEN sentiment = 'Positive' THEN rating END), 2) as avg_positive_rating,
  ROUND(AVG(CASE WHEN sentiment = 'Negative' THEN rating END), 2) as avg_negative_rating
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`;

-- Preview final dataset
SELECT 
  review_id,
  LEFT(review_text, 100) as review_preview,
  sentiment,
  rating,
  length_category,
  sentiment_rating_alignment,
  include_in_ttest
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
ORDER BY RAND()
LIMIT 10;
