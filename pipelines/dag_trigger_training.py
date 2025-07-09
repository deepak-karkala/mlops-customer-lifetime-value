from airflow import DAG
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerPipelineOperator
from datetime import datetime

with DAG(
    dag_id="clv_trigger_sagemaker_training",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=['clv', 'training', 'sagemaker'],
) as dag:
    trigger_sagemaker_pipeline = SageMakerPipelineOperator(
        task_id="trigger_training_pipeline",
        pipeline_name="CLV-Training-Pipeline",
        aws_conn_id="aws_default",
    )