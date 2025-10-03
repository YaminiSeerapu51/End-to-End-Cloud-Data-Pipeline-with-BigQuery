"""
Multi-Cloud ELT Data Warehouse Orchestration DAG
Orchestrates data pipeline across AWS Redshift, Google BigQuery, and Snowflake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

# DAG definition
dag = DAG(
    'unified_dw_pipeline',
    default_args=default_args,
    description='Multi-cloud ELT pipeline for unified data warehouse',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'multi-cloud', 'data-warehouse', 'redshift', 'bigquery', 'snowflake']
)

# =====================================================
# PHASE 1: AWS REDSHIFT - SALES DATA PROCESSING
# =====================================================

with TaskGroup("redshift_sales_processing", dag=dag) as redshift_group:
    
    # Load raw data from S3 to Redshift staging tables
    load_redshift_staging = RedshiftSQLOperator(
        task_id='load_staging_tables',
        redshift_conn_id='redshift_default',
        sql="""
        -- Load orders data
        TRUNCATE TABLE staging.orders;
        COPY staging.orders 
        FROM 's3://{{ var.value.s3_raw_bucket }}/olist-data/olist_orders_dataset.csv'
        IAM_ROLE '{{ var.value.redshift_iam_role }}'
        CSV IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD HH:MI:SS';
        
        -- Load order items data
        TRUNCATE TABLE staging.order_items;
        COPY staging.order_items 
        FROM 's3://{{ var.value.s3_raw_bucket }}/olist-data/olist_order_items_dataset.csv'
        IAM_ROLE '{{ var.value.redshift_iam_role }}'
        CSV IGNOREHEADER 1;
        
        -- Additional COPY commands for other tables...
        """,
        autocommit=True
    )
    
    # Transform raw data into processed tables
    transform_redshift_data = RedshiftSQLOperator(
        task_id='transform_sales_data',
        redshift_conn_id='redshift_default',
        sql='sql/redshift/02_transform_sales_data.sql',
        autocommit=True
    )
    
    # Export processed data to S3 as Parquet
    export_redshift_to_s3 = RedshiftSQLOperator(
        task_id='export_to_s3',
        redshift_conn_id='redshift_default',
        sql='sql/redshift/03_export_to_s3.sql',
        autocommit=True
    )
    
    # Data quality checks
    redshift_quality_check = RedshiftSQLOperator(
        task_id='data_quality_check',
        redshift_conn_id='redshift_default',
        sql="""
        -- Check record counts and data quality
        SELECT 
            CASE 
                WHEN COUNT(*) > 0 THEN 'PASS'
                ELSE 'FAIL'
            END as quality_check
        FROM processed.daily_sales_summary
        WHERE date_key = CURRENT_DATE - 1;
        """,
        autocommit=True
    )
    
    load_redshift_staging >> transform_redshift_data >> export_redshift_to_s3 >> redshift_quality_check

# =====================================================
# PHASE 2: GOOGLE BIGQUERY - MARKETING DATA PROCESSING
# =====================================================

with TaskGroup("bigquery_marketing_processing", dag=dag) as bigquery_group:
    
    # Load marketing data to BigQuery
    load_bigquery_data = BigQueryInsertJobOperator(
        task_id='load_marketing_data',
        gcp_conn_id='google_cloud_default',
        configuration={
            "load": {
                "sourceUris": ["gs://{{ var.value.gcs_raw_bucket }}/marketing-data/*.csv"],
                "destinationTable": {
                    "projectId": "{{ var.value.gcp_project_id }}",
                    "datasetId": "marketing_staging",
                    "tableId": "campaigns"
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        }
    )
    
    # Transform marketing data
    transform_bigquery_data = BigQueryInsertJobOperator(
        task_id='transform_marketing_data',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": "{% include 'sql/bigquery/02_transform_marketing_data.sql' %}",
                "useLegacySql": False
            }
        }
    )
    
    # Export to GCS as Parquet
    export_bigquery_to_gcs = BigQueryInsertJobOperator(
        task_id='export_to_gcs',
        gcp_conn_id='google_cloud_default',
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": "{{ var.value.gcp_project_id }}",
                    "datasetId": "marketing_processed",
                    "tableId": "daily_marketing_summary"
                },
                "destinationUris": ["gs://{{ var.value.gcs_clean_bucket }}/bigquery-exports/daily_marketing_summary/*.parquet"],
                "destinationFormat": "PARQUET"
            }
        }
    )
    
    # Data quality checks
    bigquery_quality_check = BigQueryInsertJobOperator(
        task_id='data_quality_check',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": """
                SELECT 
                    CASE 
                        WHEN COUNT(*) > 0 THEN 'PASS'
                        ELSE 'FAIL'
                    END as quality_check
                FROM `{{ var.value.gcp_project_id }}.marketing_processed.daily_marketing_summary`
                WHERE date_key = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                """,
                "useLegacySql": False
            }
        }
    )
    
    load_bigquery_data >> transform_bigquery_data >> export_bigquery_to_gcs >> bigquery_quality_check

# =====================================================
# PHASE 3: SNOWFLAKE - UNIFIED INTEGRATION
# =====================================================

with TaskGroup("snowflake_integration", dag=dag) as snowflake_group:
    
    # Refresh external tables to pick up new data
    refresh_external_tables = SnowflakeOperator(
        task_id='refresh_external_tables',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- Refresh external tables
        ALTER EXTERNAL TABLE EXTERNAL_SOURCES.ext_redshift_daily_sales REFRESH;
        ALTER EXTERNAL TABLE EXTERNAL_SOURCES.ext_bigquery_marketing_daily REFRESH;
        """,
        autocommit=True
    )
    
    # Generate sample product engagement data (in production, this would be real data)
    generate_engagement_data = SnowflakeOperator(
        task_id='generate_engagement_data',
        snowflake_conn_id='snowflake_default',
        sql='sql/snowflake/02_create_native_tables.sql',
        autocommit=True
    )
    
    # Create unified business KPIs table
    create_unified_kpis = SnowflakeOperator(
        task_id='create_unified_kpis',
        snowflake_conn_id='snowflake_default',
        sql='sql/snowflake/03_unified_transformation.sql',
        autocommit=True
    )
    
    # Data validation across platforms
    cross_platform_validation = SnowflakeOperator(
        task_id='cross_platform_validation',
        snowflake_conn_id='snowflake_default',
        sql="""
        -- Validate data consistency across platforms
        WITH validation_results AS (
            SELECT 
                'Revenue Consistency' as check_name,
                ABS(
                    (SELECT SUM(total_revenue) FROM EXTERNAL_SOURCES.ext_redshift_daily_sales 
                     WHERE date_key = CURRENT_DATE - 1) -
                    (SELECT SUM(total_sales_revenue) FROM ANALYTICS.fct_unified_business_kpis 
                     WHERE date_key = CURRENT_DATE - 1 AND sales_data_available)
                ) as variance
        )
        SELECT 
            CASE 
                WHEN variance < 100 THEN 'PASS'
                ELSE 'FAIL'
            END as validation_result
        FROM validation_results;
        """,
        autocommit=True
    )
    
    refresh_external_tables >> generate_engagement_data >> create_unified_kpis >> cross_platform_validation

# =====================================================
# DBT TRANSFORMATIONS
# =====================================================

# Run dbt models
run_dbt_models = BashOperator(
    task_id='run_dbt_transformations',
    bash_command="""
    cd {{ var.value.dbt_project_dir }} &&
    dbt run --profiles-dir . --target prod &&
    dbt test --profiles-dir . --target prod
    """,
    dag=dag
)

# =====================================================
# DATA QUALITY AND MONITORING
# =====================================================

def send_pipeline_summary(**context):
    """Send pipeline execution summary"""
    import logging
    
    # This would typically send metrics to monitoring systems
    # like DataDog, New Relic, or custom dashboards
    
    logging.info("Pipeline execution completed successfully")
    logging.info(f"Execution date: {context['ds']}")
    
    # In production, you would:
    # 1. Query final data counts from each platform
    # 2. Calculate data quality scores
    # 3. Send alerts if thresholds are breached
    # 4. Update monitoring dashboards
    
    return "Pipeline summary sent"

pipeline_summary = PythonOperator(
    task_id='send_pipeline_summary',
    python_callable=send_pipeline_summary,
    dag=dag
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================

# Phase dependencies (can run in parallel)
[redshift_group, bigquery_group] >> snowflake_group

# Final transformations and monitoring
snowflake_group >> run_dbt_models >> pipeline_summary

# =====================================================
# CONFIGURATION VARIABLES
# =====================================================

"""
Required Airflow Variables:
- s3_raw_bucket: S3 bucket for raw data
- s3_clean_bucket: S3 bucket for processed data
- gcs_raw_bucket: GCS bucket for raw marketing data
- gcs_clean_bucket: GCS bucket for processed data
- redshift_iam_role: IAM role for Redshift S3 access
- gcp_project_id: Google Cloud Project ID
- dbt_project_dir: Directory containing dbt project

Required Connections:
- redshift_default: AWS Redshift connection
- google_cloud_default: Google Cloud connection
- snowflake_default: Snowflake connection
"""
