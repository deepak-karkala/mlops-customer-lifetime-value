from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.models.baseoperator import chain
from datetime import datetime

# --- Constants ---
S3_BUCKET = "airflow-bucket-name" # Bucket for Airflow logs and scripts
EMR_EC2_ROLE = "emr-ec2-instance-role"
EMR_SERVICE_ROLE = "emr-service-role"
FEATURE_GROUP_NAME = "clv-feature-group-v1"
AWS_REGION = "eu-west-1"

# EMR cluster configuration
JOB_FLOW_OVERRIDES = {
    "Name": "clv-feature-engineering-cluster",
    "ReleaseLabel": "emr-6.9.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": EMR_EC2_ROLE,
    "ServiceRole": EMR_SERVICE_ROLE,
    "VisibleToAllUsers": True,
}

# Spark job steps
SPARK_STEPS = [
    {
        "Name": "Generate CLV Features",
        "ActionOnFailure": "TERMINATE_JOB_FLOW",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                f"s3://{S3_BUCKET}/scripts/generate_features.py",
                f"s3://clv-raw-data-bucket/transactional/",
                f"s3://clv-raw-data-bucket/behavioral/",
                FEATURE_GROUP_NAME,
                AWS_REGION,
            ],
        },
    }
]

with DAG(
    dag_id="clv_feature_generation_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["clv", "feature-engineering"],
) as dag:
    # Task to create a transient EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )

    # Task to add the Spark job step
    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    # Task to terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",  # Ensures cluster is terminated even if the Spark job fails
    )

    # Define task dependencies
    chain(create_emr_cluster, add_spark_step, terminate_emr_cluster)