import pytest
import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import time
import random

# Import the functions from our source script
from src.generate_features import generate_features, write_to_feature_store

# --- Fixtures for AWS Resource Management ---

@pytest.fixture(scope="session")
def spark_session():
    """Provides a Spark session for the test."""
    return SparkSession.builder.master("local[*]").appName("IntegrationTest").getOrCreate()

@pytest.fixture(scope="module")
def aws_region():
    return "eu-west-1"

@pytest.fixture(scope="module")
def sagemaker_session(aws_region):
    """Provides a SageMaker session."""
    boto_session = boto3.Session(region_name=aws_region)
    return sagemaker.Session(boto_session=boto_session)

@pytest.fixture(scope="module")
def test_s3_bucket(sagemaker_session):
    """Creates a temporary S3 bucket for the test run and cleans it up afterward."""
    bucket_name = f"clv-test-bucket-integration-{int(time.time())}-{random.randint(1000, 9999)}"
    s3 = sagemaker_session.boto_session.resource("s3")
    try:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': sagemaker_session.boto_region_name})
        print(f"Created test bucket: {bucket_name}")
        yield bucket_name
    finally:
        print(f"Cleaning up bucket: {bucket_name}")
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()

@pytest.fixture(scope="module")
def feature_group_name():
    """Generates a unique feature group name for the test run."""
    return f"clv-test-fg-{int(time.time())}"

@pytest.fixture(scope="module")
def sagemaker_feature_group(sagemaker_session, feature_group_name, aws_region):
    """Creates a SageMaker Feature Group for the test and cleans it up afterward."""
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=sagemaker_session)
    
    # Define the schema based on the output of our Spark job
    feature_definitions = [
        {"FeatureName": "CustomerID", "FeatureType": "String"},
        {"FeatureName": "recency", "FeatureType": "Integral"},
        {"FeatureName": "frequency", "FeatureType": "Integral"},
        {"FeatureName": "monetary", "FeatureType": "Fractional"},
        {"FeatureName": "T", "FeatureType": "Integral"},
        {"FeatureName": "purchase_count_30d", "FeatureType": "Integral"},
        {"FeatureName": "total_spend_30d", "FeatureType": "Fractional"},
        {"FeatureName": "purchase_count_90d", "FeatureType": "Integral"},
        {"FeatureName": "total_spend_90d", "FeatureType": "Fractional"},
        {"FeatureName": "total_sessions_90d", "FeatureType": "Integral"},
        {"FeatureName": "avg_session_duration_90d", "FeatureType": "Fractional"},
        {"FeatureName": "add_to_cart_count_90d", "FeatureType": "Integral"},
        {"FeatureName": "EventTime", "FeatureType": "String"}
    ]
    
    feature_group.feature_definitions = feature_definitions
    feature_group.record_identifier_name = "CustomerID"
    feature_group.event_time_feature_name = "EventTime"
    
    try:
        print(f"Creating Feature Group: {feature_group_name}")
        feature_group.create(
            s3_uri=f"s3://{sagemaker_session.default_bucket()}/{feature_group_name}",
            enable_online_store=True,
            role_arn="arn:aws:iam::${YOUR_ACCOUNT_ID}:role/service-role/AmazonSageMaker-ExecutionRole-..." # Replace with your actual SageMaker role ARN
        )
        # Wait for the feature group to be created
        while feature_group.describe().get("FeatureGroupStatus") == "Creating":
            print("Waiting for feature group creation...")
            time.sleep(10)
        yield feature_group_name
    finally:
        print(f"Deleting Feature Group: {feature_group_name}")
        try:
            feature_group.delete()
        except Exception as e:
            print(f"Error deleting feature group: {e}")


@pytest.fixture(scope="module")
def test_data_on_s3(spark_session, test_s3_bucket):
    """Creates mock raw data and uploads it to the test S3 bucket."""
    # Create mock transactional data
    trans_pd = pd.DataFrame({
        'CustomerID': ['101', '101', '102'],
        'InvoiceNo': ['A1', 'A2', 'B1'],
        'InvoiceDate': [datetime.now() - timedelta(days=60), datetime.now() - timedelta(days=10), datetime.now() - timedelta(days=30)],
        'SalePrice': [15.50, 25.00, 100.00]
    })
    
    # Create mock behavioral data
    behav_pd = pd.DataFrame({
        'CustomerID': ['101'],
        'session_id': ['s1'],
        'event_timestamp': [datetime.now() - timedelta(days=5)],
        'session_duration_seconds': [180],
        'event_type': ['add_to_cart']
    })
    
    trans_df = spark_session.createDataFrame(trans_pd)
    behav_df = spark_session.createDataFrame(behav_pd)

    trans_path = f"s3a://{test_s3_bucket}/transactional/"
    behav_path = f"s3a://{test_s3_bucket}/behavioral/"
    
    trans_df.write.mode("overwrite").parquet(trans_path)
    behav_df.write.mode("overwrite").parquet(behav_path)

    return trans_path, behav_path

# --- The Integration Test ---

def test_spark_job_s3_to_feature_store(spark_session, test_data_on_s3, sagemaker_feature_group, aws_region):
    """
    This test orchestrates the full feature engineering pipeline:
    1. Reads test data from a temporary S3 bucket.
    2. Runs the Spark feature generation logic.
    3. Writes the results to a temporary SageMaker Feature Group.
    4. Validates the data in the Feature Group.
    """
    # ARRANGE: Fixtures have already set up S3 data and the Feature Group.
    transactional_path, behavioral_path = test_data_on_s3
    feature_group_name = sagemaker_feature_group

    # ACT: Run the feature generation and ingestion logic.
    features_df = generate_features(spark_session, transactional_path, behavioral_path)
    write_to_feature_store(features_df, feature_group_name, aws_region)
    
    # Give a few seconds for the records to be available in the online store
    time.sleep(15)

    # ASSERT: Query the Feature Store to verify the results.
    featurestore_client = boto3.client('sagemaker-featurestore-runtime', region_name=aws_region)
    
    response = featurestore_client.get_record(
        FeatureGroupName=feature_group_name,
        RecordIdentifierValueAsString='101'
    )

    assert 'Record' in response, "Record for CustomerID 101 not found in Feature Store"
    
    # Convert the returned record to a more usable dictionary
    result_dict = {item['FeatureName']: item['ValueAsString'] for item in response['Record']}
    
    assert result_dict['CustomerID'] == '101'
    assert int(result_dict['recency']) == 50  # 60 days ago to 10 days ago
    assert int(result_dict['frequency']) == 2
    assert abs(float(result_dict['monetary']) - 20.25) < 0.01  # (15.50 + 25.00) / 2
    assert int(result_dict['purchase_count_30d']) == 1 # one purchase 10 days ago
    assert int(result_dict['add_to_cart_count_90d']) == 1