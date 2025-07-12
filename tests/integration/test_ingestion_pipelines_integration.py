# tests/integration/test_ingestion_pipelines.py
import pytest
import boto3
import time
from airflow.api.client.local_client import Client
from src.produce_behavioral_events import send_event

@pytest.mark.integration
def test_transactional_ingestion_dag():
    """Triggers the transactional data DAG and checks for completion."""
    c = Client(None, None)
    run_id = f"test_transactional_ingestion_{int(time.time())}"
    
    # Trigger and wait for the DAG to complete
    # ... (polling logic similar to previous examples) ...
    
    # Assert that the final state is 'success'
    dag_run = c.get_dag_run(dag_id="clv_ingest_transactional_data_with_validation", run_id=run_id)
    assert dag_run.state == 'success'

@pytest.mark.integration
def test_behavioral_ingestion_pipeline():
    """Sends a test event and checks if it lands in S3 via Firehose."""
    s3 = boto3.client("s3")
    bucket = "clv-raw-data-bucket-staging"
    prefix_before = f"behavioral/{datetime.now().strftime('%Y/%m/%d')}"

    # Send a test event
    test_event = {"CustomerID": 9999, "event_type": "integration_test"}
    send_event(test_event)

    # Wait for Firehose buffer to flush (e.g., 60 seconds)
    print("Waiting 65 seconds for Firehose to deliver...")
    time.sleep(65)
    
    # Check S3 for a new object
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix_before)
    assert 'Contents' in response and len(response['Contents']) > 0, "No file was delivered to S3"