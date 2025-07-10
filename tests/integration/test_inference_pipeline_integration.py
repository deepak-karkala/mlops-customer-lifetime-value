# tests/integration/test_inference_pipeline_integration.py
import pytest
import boto3
from airflow.api.client.local_client import Client
import time

DAG_ID = "clv_batch_inference_pipeline"
OUTPUT_BUCKET = "clv-inference-data-bucket-staging" # Use a staging bucket
OUTPUT_PREFIX = "output/"

@pytest.mark.integration
def test_inference_dag_end_to_end():
    # Setup: Ensure the output location is clean
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(OUTPUT_BUCKET)
    bucket.objects.filter(Prefix=OUTPUT_PREFIX).delete()
    
    # Act: Trigger the DAG
    c = Client(None, None)
    run_id = f"integration_test_{int(time.time())}"
    c.trigger_dag(dag_id=DAG_ID, run_id=run_id)

    # Poll for completion
    timeout = time.time() + 60 * 15 # 15 minute timeout
    final_state = None
    while time.time() < timeout:
        dag_run = c.get_dag_run(dag_id=DAG_ID, run_id=run_id)
        if dag_run.state in ['success', 'failed']:
            final_state = dag_run.state
            break
        print(f"DAG state is {dag_run.state}. Waiting...")
        time.sleep(30)
        
    # Assert DAG success
    assert final_state == 'success', f"DAG run failed with state: {final_state}"

    # Assert: Check if the output file was created in S3
    objects = list(bucket.objects.filter(Prefix=OUTPUT_PREFIX))
    assert len(objects) > 0, "No output file was found in the S3 bucket."
    
    print(f"Integration test passed. Output file found: {objects[0].key}")