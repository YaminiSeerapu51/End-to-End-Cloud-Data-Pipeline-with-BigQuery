-- =====================================================
-- Google Play Store Reviews Analysis - Analysis Queries
-- Phase 3: Statistical analysis and hypothesis testing preparation
-- =====================================================

-- =====================================================
-- HYPOTHESIS TESTING DATA PREPARATION
-- =====================================================

-- Query 1: Data for T-Test (Sentiment Impact on Ratings)
-- This query prepares data for Python statistical analysis
CREATE OR REPLACE VIEW `your-gcp-project-id.play_store_analysis.ttest_data` AS
SELECT 
  sentiment,
  rating,
  review_length,
  word_count,
  sentiment_score,
  length_category
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
WHERE include_in_ttest = TRUE  -- Only Positive and Negative sentiments
  AND sentiment_rating_alignment = 'Aligned'  -- Remove potentially noisy data
ORDER BY sentiment, rating;

-- Query 2: Descriptive Statistics by Sentiment
CREATE OR REPLACE VIEW `your-gcp-project-id.play_store_analysis.sentiment_descriptive_stats` AS
SELECT 
  sentiment,
  COUNT(*) as sample_size,
  ROUND(AVG(rating), 3) as mean_rating,
  ROUND(STDDEV(rating), 3) as std_rating,
  ROUND(MIN(rating), 1) as min_rating,
  ROUND(MAX(rating), 1) as max_rating,
  ROUND(APPROX_QUANTILES(rating, 4)[OFFSET(1)], 2) as q1_rating,
  ROUND(APPROX_QUANTILES(rating, 4)[OFFSET(2)], 2) as median_rating,
  ROUND(APPROX_QUANTILES(rating, 4)[OFFSET(3)], 2) as q3_rating,
  
  -- Review length statistics
  ROUND(AVG(review_length), 1) as mean_review_length,
  ROUND(STDDEV(review_length), 1) as std_review_length,
  ROUND(APPROX_QUANTILES(review_length, 4)[OFFSET(2)], 1) as median_review_length,
  
  -- Word count statistics
  ROUND(AVG(word_count), 1) as mean_word_count,
  ROUND(APPROX_QUANTILES(word_count, 4)[OFFSET(2)], 1) as median_word_count
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
WHERE include_in_ttest = TRUE
GROUP BY sentiment
ORDER BY sentiment;

-- =====================================================
-- CORRELATION ANALYSIS QUERIES
-- =====================================================

-- Query 3: Review Length vs Rating Correlation Data
CREATE OR REPLACE VIEW `your-gcp-project-id.play_store_analysis.length_rating_correlation` AS
SELECT 
  review_length,
  rating,
  sentiment,
  word_count,
  length_category,
  
  -- Binned review length for analysis
  CASE 
    WHEN review_length <= 50 THEN '0-50'
    WHEN review_length <= 100 THEN '51-100'
    WHEN review_length <= 200 THEN '101-200'
    WHEN review_length <= 400 THEN '201-400'
    ELSE '400+'
  END as length_bin,
  
  -- Rating categories for cross-tabulation
  CASE 
    WHEN rating <= 2 THEN 'Low (1-2)'
    WHEN rating = 3 THEN 'Medium (3)'
    ELSE 'High (4-5)'
  END as rating_category
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
WHERE is_length_outlier = FALSE  -- Remove extreme outliers
ORDER BY review_length;

-- Query 4: Length vs Rating Cross-Tabulation
SELECT 
  length_bin,
  rating_category,
  COUNT(*) as count,
  ROUND(AVG(rating), 2) as avg_rating,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage_of_total
FROM `your-gcp-project-id.play_store_analysis.length_rating_correlation`
GROUP BY length_bin, rating_category
ORDER BY 
  CASE length_bin
    WHEN '0-50' THEN 1
    WHEN '51-100' THEN 2
    WHEN '101-200' THEN 3
    WHEN '201-400' THEN 4
    WHEN '400+' THEN 5
  END,
  rating_category;

-- =====================================================
-- BUSINESS INSIGHTS QUERIES
-- =====================================================

-- Query 5: Review Quality Impact Analysis
SELECT 
  review_quality,
  COUNT(*) as review_count,
  ROUND(AVG(rating), 2) as avg_rating,
  ROUND(STDDEV(rating), 2) as rating_std,
  
  -- Sentiment distribution within each quality level
  ROUND(COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) as pct_positive,
  ROUND(COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) as pct_negative,
  ROUND(COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) * 100.0 / COUNT(*), 1) as pct_neutral,
  
  -- Average characteristics
  ROUND(AVG(review_length), 0) as avg_length,
  ROUND(AVG(word_count), 0) as avg_word_count
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
GROUP BY review_quality
ORDER BY 
  CASE review_quality
    WHEN 'Detailed' THEN 1
    WHEN 'Moderate' THEN 2
    WHEN 'Brief' THEN 3
  END;

-- Query 6: Sentiment-Rating Alignment Analysis
SELECT 
  sentiment_rating_alignment,
  sentiment,
  COUNT(*) as count,
  ROUND(AVG(rating), 2) as avg_rating,
  ROUND(AVG(review_length), 0) as avg_length,
  
  -- Examples of misaligned reviews for investigation
  STRING_AGG(
    CASE WHEN sentiment_rating_alignment = 'Misaligned' 
    THEN CONCAT('Rating: ', CAST(rating AS STRING), ' | ', LEFT(review_text, 50), '...') 
    END, 
    ' || ' 
    LIMIT 3
  ) as misaligned_examples
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
GROUP BY sentiment_rating_alignment, sentiment
ORDER BY sentiment_rating_alignment, sentiment;

-- =====================================================
-- DASHBOARD DATA PREPARATION
-- =====================================================

-- Query 7: Executive Summary Metrics
CREATE OR REPLACE VIEW `your-gcp-project-id.play_store_analysis.executive_summary` AS
WITH summary_metrics AS (
  SELECT 
    COUNT(*) as total_reviews,
    ROUND(AVG(rating), 2) as overall_avg_rating,
    ROUND(AVG(review_length), 0) as avg_review_length,
    COUNT(DISTINCT app_name) as apps_analyzed,
    
    -- Sentiment breakdown
    COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) as positive_reviews,
    COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) as negative_reviews,
    COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) as neutral_reviews,
    
    -- Quality breakdown
    COUNT(CASE WHEN review_quality = 'Detailed' THEN 1 END) as detailed_reviews,
    COUNT(CASE WHEN review_quality = 'Moderate' THEN 1 END) as moderate_reviews,
    COUNT(CASE WHEN review_quality = 'Brief' THEN 1 END) as brief_reviews,
    
    -- Rating distribution
    COUNT(CASE WHEN rating >= 4 THEN 1 END) as high_ratings,
    COUNT(CASE WHEN rating = 3 THEN 1 END) as medium_ratings,
    COUNT(CASE WHEN rating <= 2 THEN 1 END) as low_ratings
    
  FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
)

SELECT 
  -- Key metrics
  total_reviews,
  overall_avg_rating,
  avg_review_length,
  apps_analyzed,
  
  -- Sentiment percentages
  ROUND(positive_reviews * 100.0 / total_reviews, 1) as pct_positive,
  ROUND(negative_reviews * 100.0 / total_reviews, 1) as pct_negative,
  ROUND(neutral_reviews * 100.0 / total_reviews, 1) as pct_neutral,
  
  -- Quality percentages
  ROUND(detailed_reviews * 100.0 / total_reviews, 1) as pct_detailed,
  ROUND(moderate_reviews * 100.0 / total_reviews, 1) as pct_moderate,
  ROUND(brief_reviews * 100.0 / total_reviews, 1) as pct_brief,
  
  -- Rating percentages
  ROUND(high_ratings * 100.0 / total_reviews, 1) as pct_high_ratings,
  ROUND(medium_ratings * 100.0 / total_reviews, 1) as pct_medium_ratings,
  ROUND(low_ratings * 100.0 / total_reviews, 1) as pct_low_ratings,
  
  -- Statistical significance indicators
  CASE 
    WHEN positive_reviews >= 30 AND negative_reviews >= 30 THEN 'Sufficient for T-test'
    ELSE 'Insufficient sample size'
  END as statistical_readiness
  
FROM summary_metrics;

-- Query 8: Time-based Analysis (if timestamp data available)
SELECT 
  DATE(ingestion_timestamp) as analysis_date,
  COUNT(*) as daily_reviews,
  ROUND(AVG(rating), 2) as daily_avg_rating,
  ROUND(AVG(review_length), 0) as daily_avg_length,
  
  -- Daily sentiment distribution
  ROUND(COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) as daily_pct_positive,
  ROUND(COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) as daily_pct_negative
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
GROUP BY DATE(ingestion_timestamp)
ORDER BY analysis_date DESC;

-- =====================================================
-- STATISTICAL TEST PREPARATION QUERIES
-- =====================================================

-- Query 9: Sample Size Calculation for Statistical Power
SELECT 
  sentiment,
  COUNT(*) as sample_size,
  ROUND(AVG(rating), 3) as mean_rating,
  ROUND(STDDEV(rating), 3) as std_rating,
  
  -- Effect size calculation (Cohen's d preparation)
  ROUND(AVG(rating), 3) - LAG(ROUND(AVG(rating), 3)) OVER (ORDER BY sentiment DESC) as mean_difference,
  
  -- Statistical power indicators
  CASE 
    WHEN COUNT(*) >= 30 THEN 'Adequate for t-test'
    WHEN COUNT(*) >= 15 THEN 'Small sample - use caution'
    ELSE 'Insufficient sample size'
  END as statistical_adequacy
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
WHERE include_in_ttest = TRUE
GROUP BY sentiment
ORDER BY sentiment DESC;

-- Query 10: Outlier Detection for Statistical Analysis
SELECT 
  sentiment,
  rating,
  review_length,
  
  -- Z-score for rating (within sentiment group)
  ROUND(
    (rating - AVG(rating) OVER (PARTITION BY sentiment)) / 
    NULLIF(STDDEV(rating) OVER (PARTITION BY sentiment), 0), 
    2
  ) as rating_zscore,
  
  -- Z-score for review length (within sentiment group)
  ROUND(
    (review_length - AVG(review_length) OVER (PARTITION BY sentiment)) / 
    NULLIF(STDDEV(review_length) OVER (PARTITION BY sentiment), 0), 
    2
  ) as length_zscore,
  
  -- Outlier flags
  CASE 
    WHEN ABS(
      (rating - AVG(rating) OVER (PARTITION BY sentiment)) / 
      NULLIF(STDDEV(rating) OVER (PARTITION BY sentiment), 0)
    ) > 2 THEN TRUE 
    ELSE FALSE 
  END as is_rating_outlier,
  
  review_text
  
FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed`
WHERE include_in_ttest = TRUE
ORDER BY sentiment, ABS(rating_zscore) DESC;

-- =====================================================
-- FINAL VALIDATION QUERY
-- =====================================================

-- Query 11: Pre-Analysis Data Validation
SELECT 
  'Data Validation Summary' as validation_type,
  
  -- Sample sizes
  (SELECT COUNT(*) FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed` WHERE sentiment = 'Positive' AND include_in_ttest = TRUE) as positive_sample_size,
  (SELECT COUNT(*) FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed` WHERE sentiment = 'Negative' AND include_in_ttest = TRUE) as negative_sample_size,
  
  -- Data quality metrics
  (SELECT COUNT(*) FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed` WHERE sentiment_rating_alignment = 'Aligned') as aligned_records,
  (SELECT COUNT(*) FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed` WHERE is_length_outlier = FALSE) as non_outlier_records,
  
  -- Statistical readiness
  CASE 
    WHEN (SELECT COUNT(*) FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed` WHERE sentiment = 'Positive' AND include_in_ttest = TRUE) >= 30
     AND (SELECT COUNT(*) FROM `your-gcp-project-id.play_store_analysis.reviews_analyzed` WHERE sentiment = 'Negative' AND include_in_ttest = TRUE) >= 30
    THEN 'READY FOR STATISTICAL ANALYSIS'
    ELSE 'NEED MORE DATA'
  END as analysis_readiness,
  
  CURRENT_TIMESTAMP() as validation_timestamp;
