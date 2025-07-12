# pipelines/dag_ab_test_setup.py
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.sns.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param
from datetime import datetime
import mlflow
import pandas as pd
import numpy as np

# --- Constants ---
MLFLOW_TRACKING_URI = "http://your-mlflow-server:5000"
MODEL_NAME = "clv-prediction-model"
BASE_S3_PATH = "s3://clv-inference-data-bucket/ab-tests"
CUSTOMER_LIST_PATH = "s3://clv-inference-data-bucket/input/active_customers.jsonl"
SNS_TOPIC_ARN = "arn:aws:sns:eu-west-1:ACCOUNT_ID:clv-ab-test-notifications"

@dag(
    dag_id="clv_ab_test_setup",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manually triggered for major tests
    catchup=False,
    params={"challenger_version": Param(type="string", description="The challenger model version from MLflow to test.")},
    doc_md="Sets up a formal A/B test by scoring all users with Champion and Challenger models and assigning them to groups.",
    tags=['clv', 'testing', 'ab-test'],
)
def ab_test_setup_dag():

    @task
    def get_model_uris(**kwargs) -> dict:
        """Fetches S3 URIs for the Production (Champion) and specified Challenger models."""
        # Logic to fetch champion and challenger URIs from MLflow
        # ... (similar to shadow DAG) ...
        pass

    @task
    def assign_users_to_groups(run_id: str) -> str:
        """Randomly assigns all active customers to a 'control' or 'treatment' group."""
        print("Assigning users to A/B test groups...")
        s3_hook = S3Hook()
        customer_file = s3_hook.download_file(key=CUSTOMER_LIST_PATH)
        customers_df = pd.read_json(customer_file, lines=True)

        # Assign each user to a group
        np.random.seed(42) # for reproducibility
        customers_df['group'] = np.random.choice(['control', 'treatment'], size=len(customers_df), p=[0.5, 0.5])
        
        output_path = f"ab-tests/{run_id}/assignments/user_assignments.csv"
        s3_hook.load_string(
            string_data=customers_df[['CustomerID', 'group']].to_csv(index=False),
            key=output_path,
            bucket_name="clv-inference-data-bucket",
            replace=True
        )
        print(f"User assignments saved to {output_path}")
        return output_path

    model_uris = get_model_uris()
    user_assignments_path = assign_users_to_groups(run_id="{{ run_id }}")
    
    # Run two parallel batch transform jobs
    run_champion_job = SageMakerTransformOperator(...)
    run_challenger_job = SageMakerTransformOperator(...)
    
    @task
    def notify_stakeholders(run_id: str):
        """Sends a notification that the A/B test setup is complete."""
        message = f"""
        A/B Test Setup Complete for run_id: {run_id}

        The following artifacts are ready for the Marketing and Analytics teams:
        - User Group Assignments: s3://clv-inference-data-bucket/ab-tests/{run_id}/assignments/
        - Champion Model Scores: {run_champion_job.output_path}
        - Challenger Model Scores: {run_challenger_job.output_path}
        
        The experiment can now begin.
        """
        return message

    notification = notify_stakeholders(run_id="{{ run_id }}")

    publish_notification = SnsPublishOperator(
        task_id="publish_setup_complete_notification",
        target_arn=SNS_TOPIC_ARN,
        message=notification,
    )
    
    # Dependencies
    [run_champion_job, run_challenger_job]
    user_assignments_path >> [run_champion_job, run_challenger_job]
    model_uris >> [run_champion_job, run_challenger_job]
    [run_champion_job, run_challenger_job] >> notification >> publish_notification

ab_test_setup_dag()