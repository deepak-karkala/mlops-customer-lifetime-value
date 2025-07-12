# pipelines/dag_monitor_drift.py
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.sns.operators.sns import SnsPublishOperator
import json
from datetime import datetime

@dag(
    dag_id="clv_monitor_production_drift",
    start_date=datetime(2025, 1, 1),
    # Runs after the weekly inference pipeline
    schedule_interval="@weekly",
    catchup=False,
    tags=['clv', 'monitoring'],
)
def monitor_drift_dag():
    
    run_drift_check_job = SageMakerProcessingOperator(
        task_id="run_drift_check_job",
        config={
            # SageMaker processing job config pointing to src/run_drift_check.py
        },
        # ... other params
    )
    
    @task
    def check_drift_report_and_alert(report_s3_path: str):
        s3_hook = S3Hook(aws_conn_id="aws_default")
        report_str = s3_hook.read_key(key=report_s3_path, bucket_name="clv-monitoring-bucket")
        report = json.loads(report_str)
        
        prediction_psi = report.get("prediction_drift", {}).get("psi", 0)
        
        if prediction_psi > 0.25:
            return "trigger_high_priority_alert"
        elif prediction_psi > 0.1:
            return "trigger_medium_priority_alert"
        else:
            return "skip_alerting"

    high_priority_alert = SnsPublishOperator(
        task_id="trigger_high_priority_alert",
        target_arn="arn:aws:sns:eu-west-1:ACCOUNT_ID:pagerduty-critical-alerts",
        message="CRITICAL: Significant prediction drift detected in CLV model!",
    )
    # ... define medium priority alert ...
    
    check_task = check_drift_report_and_alert(run_drift_check_job.output_path)
    run_drift_check_job >> check_task >> [high_priority_alert] # Add other alerts

monitor_drift_dag()