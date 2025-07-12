# pipelines/dag_shadow_deployment.py
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param
from datetime import datetime
import mlflow
import json
import pandas as pd

MLFLOW_TRACKING_URI = "http://your-mlflow-server:5000"
MODEL_NAME = "clv-prediction-model"
SAGEMAKER_ROLE = "arn:aws:iam::ACCOUNT_ID:role/sagemaker-inference-execution-role"
INPUT_S3_URI = "s3://clv-inference-data-bucket/input/active_customers.jsonl"

@dag(
    dag_id="clv_shadow_deployment",
    start_date=datetime(2025, 1, 1),
    schedule=None, # Triggered by the retraining DAG
    catchup=False,
    params={"model_version": Param(None, type=["null", "string"])},
    doc_md="Compares a 'Staging' challenger model against the 'Production' champion.",
    tags=['clv', 'testing', 'lifecycle'],
)
def shadow_deployment_dag():

    @task
    def get_model_uris(**kwargs) -> dict:
        """Fetches S3 URIs for both the Production and new Staging models."""
        client = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
        
        # Get Production model
        prod_model = client.get_latest_versions(MODEL_NAME, stages=["Production"])[0]
        
        # Get Challenger (Staging) model version from the trigger payload
        challenger_version = kwargs["params"]["model_version"]
        if not challenger_version:
            raise ValueError("No model_version passed in the trigger config.")
            
        challenger_model = client.get_model_version(MODEL_NAME, challenger_version)

        print(f"Champion Model: v{prod_model.version}")
        print(f"Challenger Model: v{challenger_model.version}")
        
        return {
            "champion_uri": prod_model.source,
            "challenger_uri": challenger_model.source,
            "challenger_version": challenger_model.version
        }
    
    model_uris = get_model_uris()

    # Assume SageMaker model objects are created/updated based on these URIs
    # These tasks run the inference jobs
    run_champion_inference = SageMakerTransformOperator(
        task_id="run_champion_inference",
        # Config would point to the champion model object
    )
    
    run_challenger_inference = SageMakerTransformOperator(
        task_id="run_challenger_inference",
        # Config would point to the challenger model object
    )

    @task
    def compare_shadow_results(champion_output: str, challenger_output: str, challenger_version: str):
        """Compares prediction distributions from both models."""
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        # This is simplified. In reality, you'd download the files from S3.
        champion_preds = pd.read_json(champion_output, lines=True)
        challenger_preds = pd.read_json(challenger_output, lines=True)

        champion_mean = champion_preds['CLV_Prediction'].mean()
        challenger_mean = challenger_preds['CLV_Prediction'].mean()
        
        percent_diff = abs(challenger_mean - champion_mean) / champion_mean
        
        print(f"Champion Mean Prediction: {champion_mean:.2f}")
        print(f"Challenger Mean Prediction: {challenger_mean:.2f}")
        print(f"Mean Percentage Difference: {percent_diff:.2%}")
        
        # Validation Gate: Pass if the mean prediction is within 15%
        if percent_diff < 0.15:
            print("Shadow test PASSED. Promoting model to 'Ready-for-Canary'.")
            client = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
            client.transition_model_version_stage(
                name=MODEL_NAME, version=challenger_version, stage="Ready-for-Canary"
            )
        else:
            print("Shadow test FAILED. Mean prediction shifted too much.")
            # Trigger failure alert here (e.g., via SnsPublishOperator)
            raise ValueError("Shadow test failed.")

    # Define dependencies
    [run_champion_inference, run_challenger_inference] >> compare_shadow_results(
        champion_output=run_champion_inference.output_path,
        challenger_output=run_challenger_inference.output_path,
        challenger_version=model_uris["challenger_version"],
    )

shadow_deployment_dag()