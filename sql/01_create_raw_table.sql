-- =====================================================
-- Google Play Store Reviews Analysis - Raw Table Creation
-- Phase 1: Create the raw table structure in BigQuery
-- =====================================================

-- Create dataset if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `your-gcp-project-id.play_store_analysis`
OPTIONS (
  description = "Dataset for Google Play Store reviews analysis project",
  location = "US"
);

-- Use the dataset
USE `your-gcp-project-id.play_store_analysis`;

-- =====================================================
-- RAW REVIEWS TABLE
-- =====================================================

-- Create raw reviews table (this will be populated by the Python ingestion script)
CREATE OR REPLACE TABLE `your-gcp-project-id.play_store_analysis.raw_reviews` (
  review_text STRING NOT NULL,
  sentiment STRING NOT NULL,
  rating FLOAT64 NOT NULL,
  app_name STRING,
  review_length INT64,
  review_word_count INT64,
  review_length_category STRING,
  rating_category STRING,
  sentiment_binary FLOAT64,
  ingestion_timestamp TIMESTAMP,
  data_source STRING
)
PARTITION BY DATE(ingestion_timestamp)
CLUSTER BY sentiment, rating
OPTIONS (
  description = "Raw Google Play Store reviews data with basic cleaning and feature engineering",
  partition_expiration_days = 90
);

-- =====================================================
-- DATA QUALITY CHECKS FOR RAW TABLE
-- =====================================================

-- Check if table exists and has data
SELECT 
  'raw_reviews' as table_name,
  COUNT(*) as total_rows,
  COUNT(DISTINCT sentiment) as unique_sentiments,
  MIN(rating) as min_rating,
  MAX(rating) as max_rating,
  AVG(review_length) as avg_review_length,
  MIN(ingestion_timestamp) as earliest_ingestion,
  MAX(ingestion_timestamp) as latest_ingestion
FROM `your-gcp-project-id.play_store_analysis.raw_reviews`;

-- Check sentiment distribution
SELECT 
  sentiment,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `your-gcp-project-id.play_store_analysis.raw_reviews`
GROUP BY sentiment
ORDER BY count DESC;

-- Check rating distribution
SELECT 
  rating,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM `your-gcp-project-id.play_store_analysis.raw_reviews`
GROUP BY rating
ORDER BY rating;

-- Check for data quality issues
SELECT 
  'Data Quality Summary' as check_type,
  COUNT(*) as total_records,
  COUNT(CASE WHEN review_text IS NULL OR LENGTH(review_text) = 0 THEN 1 END) as empty_reviews,
  COUNT(CASE WHEN sentiment IS NULL THEN 1 END) as missing_sentiment,
  COUNT(CASE WHEN rating IS NULL OR rating < 1 OR rating > 5 THEN 1 END) as invalid_ratings,
  COUNT(CASE WHEN review_length IS NULL OR review_length <= 0 THEN 1 END) as invalid_length
FROM `your-gcp-project-id.play_store_analysis.raw_reviews`;

-- Sample data preview
SELECT 
  review_text,
  sentiment,
  rating,
  review_length,
  review_length_category,
  rating_category
FROM `your-gcp-project-id.play_store_analysis.raw_reviews`
LIMIT 10;
