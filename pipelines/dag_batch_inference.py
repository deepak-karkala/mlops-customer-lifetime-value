# pipelines/dag_batch_inference.py
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.models.param import Param
from datetime import datetime
import mlflow

# --- Constants ---
MLFLOW_TRACKING_URI = "http://your-mlflow-server:5000" # Should be an Airflow Variable/Connection
MODEL_NAME = "clv-prediction-model"
SAGEMAKER_ROLE = "arn:aws:iam::ACCOUNT_ID:role/sagemaker-inference-execution-role"
INPUT_S3_URI = "s3://clv-inference-data-bucket/input/active_customers.jsonl"
OUTPUT_S3_URI = "s3://clv-inference-data-bucket/output/"

@dag(
    dag_id="clv_batch_inference_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    doc_md="DAG to run weekly batch inference for CLV prediction.",
    tags=["clv", "inference"],
)
def batch_inference_dag():

    @task
    def get_production_model_uri() -> str:
        """Fetches the S3 URI of the latest model in the 'Production' stage."""
        client = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
        latest_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        if not latest_versions:
            raise ValueError(f"No model in Production stage found for '{MODEL_NAME}'")
        
        prod_model = latest_versions[0]
        print(f"Found production model: Version {prod_model.version}, URI: {prod_model.source}")
        return prod_model.source

    model_uri = get_production_model_uri()
    
    # In a real pipeline, the model object in SageMaker should be created separately.
    # For this operator, we assume a SageMaker model object exists.
    # The operator just creates the transform job.
    run_batch_transform = SageMakerTransformOperator(
        task_id="run_sagemaker_batch_transform",
        config={
            "TransformJobName": f"clv-batch-inference-{{{{ ds_nodash }}}}",
            "ModelName": "clv-sagemaker-model-name", # Name of the model in SageMaker
            "TransformInput": {
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": INPUT_S3_URI}},
                "ContentType": "application/json",
                "SplitType": "Line",
            },
            "TransformOutput": {
                "S3OutputPath": OUTPUT_S3_URI,
                "Accept": "application/json",
                "AssembleWith": "Line",
            },
            "TransformResources": {"InstanceType": "ml.m5.large", "InstanceCount": 1},
        },
        aws_conn_id="aws_default",
    )
    
    # This task would load the S3 output into the data warehouse
    @task
    def load_predictions_to_dwh(s3_output_path: str):
        print(f"Simulating load of data from {s3_output_path} into Data Warehouse.")
        # Add pandas/boto3 logic here to read from S3 and write to Redshift/Snowflake
        return "Load complete."

    model_uri >> run_batch_transform >> load_predictions_to_dwh(OUTPUT_S3_URI)

batch_inference_dag()