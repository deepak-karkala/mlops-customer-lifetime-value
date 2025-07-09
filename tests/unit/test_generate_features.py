import pytest
from pyspark.sql import SparkSession
from src.generate_features import generate_features
import pandas as pd
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def spark_session():
    """Creates a Spark session for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("CLVFeatureTests") \
        .getOrCreate()

def test_feature_generation_logic(spark_session):
    """Tests the end-to-end feature generation function."""
    # Create mock transactional data
    trans_pd = pd.DataFrame({
        'CustomerID': [1, 1, 2, 1],
        'InvoiceNo': ['A1', 'A2', 'B1', 'A3'],
        'InvoiceDate': [
            datetime.now() - timedelta(days=100),
            datetime.now() - timedelta(days=50),
            datetime.now() - timedelta(days=20),
            datetime.now() - timedelta(days=10)
        ],
        'SalePrice': [10.0, 20.0, 50.0, 30.0]
    })
    # Create mock behavioral data
    behav_pd = pd.DataFrame({
        'CustomerID': [1, 2],
        'session_id': ['s1', 's2'],
        'event_timestamp': [datetime.now() - timedelta(days=5), datetime.now() - timedelta(days=15)],
        'session_duration_seconds': [120, 300],
        'event_type': ['add_to_cart', 'page_view']
    })

    trans_df = spark_session.createDataFrame(trans_pd)
    behav_df = spark_session.createDataFrame(behav_pd)
    
    # Create temporary paths for the test data
    trans_path = "file:///tmp/test_trans_data"
    behav_path = "file:///tmp/test_behav_data"
    trans_df.write.mode("overwrite").parquet(trans_path)
    behav_df.write.mode("overwrite").parquet(behav_path)

    # Run the feature generation function
    features_df = generate_features(spark_session, trans_path, behav_path)
    
    # Collect results for validation
    result = features_df.filter(col("CustomerID") == 1).first()

    assert result is not None
    assert result['CustomerID'] == 1
    assert result['recency'] == 90  # 100 days ago to 10 days ago
    assert result['T'] >= 100
    assert result['frequency'] == 3
    assert abs(result['monetary'] - 20.0) < 0.01 # (10+20+30)/3
    assert result['purchase_count_30d'] == 1
    assert result['add_to_cart_count_90d'] == 1