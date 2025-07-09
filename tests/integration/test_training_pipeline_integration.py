import pytest
import boto3
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
import time

@pytest.mark.integration
def test_training_pipeline_dag_runs_successfully():
    """
    Tests the end-to-end execution by triggering the Airflow DAG
    and monitoring the SageMaker Pipeline.
    """
    # Load the Airflow DAG
    dagbag = DagBag(dag_folder='pipelines/', include_examples=False)
    dag = dagbag.get_dag('clv_trigger_sagemaker_training')
    assert dag is not None, "DAG not found"

    # Manually trigger the DAG
    dag.clear()
    dag_run = dag.create_dagrun(
        state=State.RUNNING,
        run_id=f"test_run_{int(time.time())}",
        conf={},
    )
    
    print(f"Triggered DAG run: {dag_run.run_id}")

    # Monitor the DAG run and the underlying SageMaker Pipeline
    # This is a simplified check. A real-world test would need more robust polling.
    sagemaker_client = boto3.client("sagemaker")
    
    time.sleep(60) # Give Airflow time to start the SageMaker Pipeline

    # Find the latest pipeline execution
    executions = sagemaker_client.list_pipeline_executions(
        PipelineName="CLV-Training-Pipeline",
        SortBy="CreationTime",
        SortOrder="Descending"
    )['PipelineExecutionSummaries']
    
    assert len(executions) > 0, "No SageMaker pipeline execution found"
    
    latest_execution_arn = executions[0]['PipelineExecutionArn']
    print(f"Monitoring SageMaker Pipeline execution: {latest_execution_arn}")

    # Poll until the pipeline execution is complete
    timeout = time.time() + 60*30 # 30 minutes timeout
    while time.time() < timeout:
        response = sagemaker_client.describe_pipeline_execution(
            PipelineExecutionArn=latest_execution_arn
        )
        status = response['PipelineExecutionStatus']
        if status in ['Succeeded', 'Failed', 'Stopped']:
            break
        print(f"Pipeline status: {status}. Waiting...")
        time.sleep(30)

    assert status == 'Succeeded', f"SageMaker pipeline did not succeed. Final status: {status}"

    # ASSERTION: Check if a new model was registered in the MLflow Model Registry
    # (This would require MLflow client setup and querying logic)
    # mlflow_client = mlflow.tracking.MlflowClient()
    # versions = mlflow_client.get_latest_versions("clv-model")
    # assert len(versions) > 0, "No model was registered in MLflow"