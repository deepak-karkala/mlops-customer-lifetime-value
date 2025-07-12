# pipelines/dag_promote_to_production.py
from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime
import mlflow

@dag(
    dag_id="clv_promote_model_to_production",
    start_date=datetime(2025, 1, 1),
    schedule=None, # Manually triggered only
    catchup=False,
    params={"model_version_to_promote": Param(description="The model version to promote to Production", type="string")},
    doc_md="Manual gate to promote a validated model version to 'Production'.",
    tags=['clv', 'promotion', 'lifecycle'],
)
def promote_to_production_dag():

    @task
    def promote_model(**kwargs):
        version = kwargs["params"]["model_version_to_promote"]
        print(f"Promoting model version {version} to 'Production'...")
        client = mlflow.tracking.MlflowClient(tracking_uri="http://your-mlflow-server:5000")
        client.transition_model_version_stage(
            name="clv-prediction-model",
            version=version,
            stage="Production",
            archive_existing_versions=True
        )
        print("Promotion successful.")

    promote_model()

promote_to_production_dag()