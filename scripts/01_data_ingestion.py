"""
Google Play Store Reviews Analysis - Data Ingestion Script
Phase 1: Extract data from Kaggle, clean, and load into Google BigQuery

Author: Data Analyst
Date: 2024
"""

import pandas as pd
import numpy as np
from google.cloud import storage
from google.cloud import bigquery
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlayStoreDataIngestion:
    """
    Class to handle data ingestion from Kaggle to Google BigQuery
    """
    
    def __init__(self, project_id, bucket_name, dataset_name):
        """
        Initialize the data ingestion pipeline
        
        Args:
            project_id (str): Google Cloud Project ID
            bucket_name (str): GCS bucket name
            dataset_name (str): BigQuery dataset name
        """
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dataset_name = dataset_name
        
        # Initialize clients
        self.storage_client = storage.Client(project=project_id)
        self.bigquery_client = bigquery.Client(project=project_id)
        
        logger.info(f"Initialized data ingestion for project: {project_id}")
    
    def load_and_clean_data(self, file_path):
        """
        Load data from CSV and perform initial cleaning
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        logger.info(f"Loading data from: {file_path}")
        
        try:
            # Load the dataset
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
            
            # Display basic info about the dataset
            logger.info(f"Dataset shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")
            
            # Basic cleaning steps
            df_clean = self.clean_dataframe(df)
            
            logger.info(f"After cleaning: {len(df_clean)} rows remaining")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def clean_dataframe(self, df):
        """
        Perform data cleaning operations
        
        Args:
            df (pd.DataFrame): Raw dataframe
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        logger.info("Starting data cleaning process...")
        
        # Make a copy to avoid modifying original
        df_clean = df.copy()
        
        # Standardize column names (common variations in Play Store datasets)
        column_mapping = {
            'Translated_Review': 'review_text',
            'translated_review': 'review_text',
            'Review': 'review_text',
            'review': 'review_text',
            'Sentiment': 'sentiment',
            'sentiment': 'sentiment',
            'Rating': 'rating',
            'rating': 'rating',
            'Star': 'rating',
            'star': 'rating',
            'App': 'app_name',
            'app': 'app_name',
            'App_Name': 'app_name'
        }
        
        # Rename columns if they exist
        for old_name, new_name in column_mapping.items():
            if old_name in df_clean.columns:
                df_clean = df_clean.rename(columns={old_name: new_name})
                logger.info(f"Renamed column: {old_name} -> {new_name}")
        
        # Ensure we have the required columns
        required_columns = ['review_text', 'sentiment', 'rating']
        missing_columns = [col for col in required_columns if col not in df_clean.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            logger.info(f"Available columns: {list(df_clean.columns)}")
            raise ValueError(f"Dataset must contain columns: {required_columns}")
        
        # Data cleaning operations
        initial_rows = len(df_clean)
        
        # Remove rows with missing review text
        df_clean = df_clean.dropna(subset=['review_text'])
        logger.info(f"Removed {initial_rows - len(df_clean)} rows with missing review text")
        
        # Remove empty or very short reviews (less than 3 characters)
        df_clean = df_clean[df_clean['review_text'].str.len() >= 3]
        logger.info(f"Removed rows with very short reviews. Remaining: {len(df_clean)}")
        
        # Clean rating column - ensure it's numeric and within valid range (1-5)
        df_clean['rating'] = pd.to_numeric(df_clean['rating'], errors='coerce')
        df_clean = df_clean.dropna(subset=['rating'])
        df_clean = df_clean[(df_clean['rating'] >= 1) & (df_clean['rating'] <= 5)]
        logger.info(f"Cleaned ratings. Remaining rows: {len(df_clean)}")
        
        # Clean sentiment column
        df_clean['sentiment'] = df_clean['sentiment'].str.strip().str.title()
        valid_sentiments = ['Positive', 'Negative', 'Neutral']
        df_clean = df_clean[df_clean['sentiment'].isin(valid_sentiments)]
        logger.info(f"Cleaned sentiment. Remaining rows: {len(df_clean)}")
        
        # Add additional features
        df_clean = self.add_features(df_clean)
        
        # Add metadata columns
        df_clean['ingestion_timestamp'] = datetime.now()
        df_clean['data_source'] = 'kaggle_google_play_store'
        
        logger.info(f"Data cleaning completed. Final dataset: {len(df_clean)} rows")
        return df_clean
    
    def add_features(self, df):
        """
        Add engineered features to the dataframe
        
        Args:
            df (pd.DataFrame): Cleaned dataframe
            
        Returns:
            pd.DataFrame: Dataframe with additional features
        """
        logger.info("Adding engineered features...")
        
        # Review length (character count)
        df['review_length'] = df['review_text'].str.len()
        
        # Review word count
        df['review_word_count'] = df['review_text'].str.split().str.len()
        
        # Review length categories
        df['review_length_category'] = pd.cut(
            df['review_length'], 
            bins=[0, 50, 150, 500, float('inf')], 
            labels=['Short', 'Medium', 'Long', 'Very Long']
        )
        
        # Rating categories
        df['rating_category'] = pd.cut(
            df['rating'],
            bins=[0, 2, 3, 4, 5],
            labels=['Poor', 'Fair', 'Good', 'Excellent']
        )
        
        # Binary sentiment (for statistical testing)
        df['sentiment_binary'] = df['sentiment'].map({
            'Positive': 1,
            'Negative': 0,
            'Neutral': 0.5
        })
        
        logger.info("Feature engineering completed")
        return df
    
    def upload_to_gcs(self, df, blob_name):
        """
        Upload dataframe to Google Cloud Storage
        
        Args:
            df (pd.DataFrame): Dataframe to upload
            blob_name (str): Name of the blob in GCS
        """
        logger.info(f"Uploading data to GCS bucket: {self.bucket_name}")
        
        try:
            # Get bucket
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_name)
            
            # Convert dataframe to CSV string
            csv_data = df.to_csv(index=False)
            
            # Upload to GCS
            blob.upload_from_string(csv_data, content_type='text/csv')
            
            logger.info(f"Successfully uploaded {len(df)} rows to gs://{self.bucket_name}/{blob_name}")
            
        except Exception as e:
            logger.error(f"Error uploading to GCS: {str(e)}")
            raise
    
    def create_bigquery_dataset(self):
        """
        Create BigQuery dataset if it doesn't exist
        """
        try:
            dataset_id = f"{self.project_id}.{self.dataset_name}"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset.description = "Google Play Store Reviews Analysis Dataset"
            
            dataset = self.bigquery_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created/verified dataset: {dataset_id}")
            
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            raise
    
    def load_to_bigquery(self, gcs_uri, table_name):
        """
        Load data from GCS to BigQuery
        
        Args:
            gcs_uri (str): GCS URI of the data file
            table_name (str): BigQuery table name
        """
        logger.info(f"Loading data to BigQuery table: {table_name}")
        
        try:
            # Create dataset if it doesn't exist
            self.create_bigquery_dataset()
            
            # Configure the load job
            table_id = f"{self.project_id}.{self.dataset_name}.{table_name}"
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header row
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            
            # Start the load job
            load_job = self.bigquery_client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
            )
            
            # Wait for the job to complete
            load_job.result()
            
            # Get table info
            table = self.bigquery_client.get_table(table_id)
            logger.info(f"Successfully loaded {table.num_rows} rows to {table_id}")
            
        except Exception as e:
            logger.error(f"Error loading to BigQuery: {str(e)}")
            raise
    
    def validate_data_quality(self, table_name):
        """
        Perform data quality checks on the loaded table
        
        Args:
            table_name (str): BigQuery table name
        """
        logger.info("Performing data quality validation...")
        
        table_id = f"{self.project_id}.{self.dataset_name}.{table_name}"
        
        # Quality check queries
        quality_checks = [
            {
                'name': 'Total Row Count',
                'query': f"SELECT COUNT(*) as count FROM `{table_id}`"
            },
            {
                'name': 'Null Review Text Count',
                'query': f"SELECT COUNT(*) as count FROM `{table_id}` WHERE review_text IS NULL"
            },
            {
                'name': 'Rating Distribution',
                'query': f"SELECT rating, COUNT(*) as count FROM `{table_id}` GROUP BY rating ORDER BY rating"
            },
            {
                'name': 'Sentiment Distribution',
                'query': f"SELECT sentiment, COUNT(*) as count FROM `{table_id}` GROUP BY sentiment"
            },
            {
                'name': 'Average Review Length',
                'query': f"SELECT AVG(review_length) as avg_length FROM `{table_id}`"
            }
        ]
        
        # Execute quality checks
        for check in quality_checks:
            try:
                query_job = self.bigquery_client.query(check['query'])
                results = query_job.result()
                
                logger.info(f"\n{check['name']}:")
                for row in results:
                    logger.info(f"  {dict(row)}")
                    
            except Exception as e:
                logger.error(f"Error in quality check '{check['name']}': {str(e)}")
    
    def run_pipeline(self, csv_file_path):
        """
        Run the complete data ingestion pipeline
        
        Args:
            csv_file_path (str): Path to the source CSV file
        """
        logger.info("Starting data ingestion pipeline...")
        
        try:
            # Step 1: Load and clean data
            df_clean = self.load_and_clean_data(csv_file_path)
            
            # Step 2: Upload to GCS
            blob_name = "raw_data/google_play_reviews_cleaned.csv"
            self.upload_to_gcs(df_clean, blob_name)
            
            # Step 3: Load to BigQuery
            gcs_uri = f"gs://{self.bucket_name}/{blob_name}"
            table_name = "raw_reviews"
            self.load_to_bigquery(gcs_uri, table_name)
            
            # Step 4: Validate data quality
            self.validate_data_quality(table_name)
            
            logger.info("Data ingestion pipeline completed successfully!")
            
            # Print summary
            print("\n" + "="*50)
            print("DATA INGESTION SUMMARY")
            print("="*50)
            print(f"✅ Processed {len(df_clean)} reviews")
            print(f"✅ Uploaded to GCS: gs://{self.bucket_name}/{blob_name}")
            print(f"✅ Loaded to BigQuery: {self.project_id}.{self.dataset_name}.{table_name}")
            print(f"✅ Data quality validation completed")
            print("="*50)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


def main():
    """
    Main function to run the data ingestion pipeline
    """
    # Configuration - UPDATE THESE VALUES
    PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
    BUCKET_NAME = "your-bucket-name"    # Replace with your GCS bucket name
    DATASET_NAME = "play_store_analysis" # BigQuery dataset name
    CSV_FILE_PATH = "../data/google_play_store_reviews.csv"  # Path to your Kaggle dataset
    
    # Verify environment
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV file not found: {CSV_FILE_PATH}")
        print("Please download the Google Play Store reviews dataset from Kaggle")
        print("Suggested datasets:")
        print("- https://www.kaggle.com/datasets/lava18/google-play-store-apps")
        print("- https://www.kaggle.com/datasets/prakharrathi25/google-play-store-reviews")
        return
    
    # Check for Google Cloud credentials
    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        print("❌ Google Cloud credentials not found")
        print("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        print("or run: gcloud auth application-default login")
        return
    
    try:
        # Initialize and run pipeline
        ingestion = PlayStoreDataIngestion(PROJECT_ID, BUCKET_NAME, DATASET_NAME)
        ingestion.run_pipeline(CSV_FILE_PATH)
        
        print("\n🎉 SUCCESS! Your data is now ready for analysis in BigQuery")
        print(f"Next step: Run the SQL transformations in BigQuery console")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        print("Please check the logs above for detailed error information")


if __name__ == "__main__":
    main()
