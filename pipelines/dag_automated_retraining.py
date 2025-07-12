# pipelines/dag_automated_retraining.py
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerPipelineOperator
from datetime import datetime
import mlflow

MLFLOW_TRACKING_URI = "http://your-mlflow-server:5000"
MODEL_NAME = "clv-prediction-model"

@dag(
    dag_id="clv_automated_retraining",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # This DAG is externally triggered
    catchup=False,
    doc_md="""
    Triggered by monitoring alerts (drift, performance degradation).
    Runs the SageMaker training pipeline and registers a new challenger model.
    On success, triggers the shadow deployment validation pipeline.
    """,
    tags=['clv', 'retraining', 'lifecycle'],
)
def automated_retraining_dag():

    # This operator starts the SageMaker Pipeline we defined in the previous section.
    trigger_sagemaker_training = SageMakerPipelineOperator(
        task_id="trigger_sagemaker_training_pipeline",
        pipeline_name="CLV-Training-Pipeline",
        aws_conn_id="aws_default",
        wait_for_completion=True, # Ensure we wait until it's done
    )
    
    @task
    def get_latest_model_version_and_promote_to_staging() -> str:
        """
        After training, the SageMaker pipeline registers a model. This task finds
        the latest version, assumes it's the one we just trained, and promotes
        it to the 'Staging' stage in MLflow.
        """
        client = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
        # Find the most recently registered version of our model
        latest_version = client.get_latest_versions(MODEL_NAME, stages=["None"])[0]
        
        print(f"Found new model version: {latest_version.version}. Transitioning to 'Staging'.")
        
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=latest_version.version,
            stage="Staging",
            archive_existing_versions=True # Demotes any existing model in 'Staging'
        )
        return latest_version.version

    # Trigger the shadow test pipeline, passing the new model version for validation.
    trigger_shadow_test = TriggerDagRunOperator(
        task_id="trigger_shadow_deployment",
        trigger_dag_id="clv_shadow_deployment",
        conf={"model_version": "{{ task_instance.xcom_pull(task_ids='get_latest_model_version_and_promote_to_staging') }}"}
    )

    new_model_version = get_latest_model_version_and_promote_to_staging()
    
    trigger_sagemaker_training >> new_model_version >> trigger_shadow_test

automated_retraining_dag()