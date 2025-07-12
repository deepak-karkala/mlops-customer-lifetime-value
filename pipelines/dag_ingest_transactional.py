# pipelines/dag_ingest_transactional.py

from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from datetime import datetime

# Path to your Great Expectations project
GE_PROJECT_ROOT_DIR = "/usr/local/airflow/include/great_expectations"

@dag(
    dag_id="clv_ingest_transactional_data_with_validation",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['clv', 'ingestion', 'data-quality'],
)
def ingest_transactional_data_dag():
    
    run_glue_job = GlueJobOperator(
        task_id="extract_and_load_raw_data_to_s3",
        job_name="clv_ingest_transactional_job",
        # ... other params
    )

    validate_raw_data = GreatExpectationsOperator(
        task_id="validate_raw_data",
        data_context_root_dir=GE_PROJECT_ROOT_DIR,
        # Define a checkpoint that knows how to validate our S3 data
        checkpoint_name="s3_transactional_data_checkpoint",
        fail_task_on_validation_failure=True, # Critical: stops the pipeline on bad data
    )

    run_glue_job >> validate_raw_data

ingest_transactional_data_dag()