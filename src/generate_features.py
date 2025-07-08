# src/generate_features.py

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, min, countDistinct, sum, avg, datediff, expr, window
from pyspark.sql.types import TimestampType
import boto3

def get_sagemaker_feature_store_client(region):
    """Initializes the SageMaker Feature Store runtime client."""
    return boto3.client('sagemaker-featurestore-runtime', region_name=region)

def generate_features(spark, transactional_data_path, behavioral_data_path):
    """
    Reads raw data and generates a customer feature set.
    """
    # Load raw data
    trans_df = spark.read.parquet(transactional_data_path)
    behav_df = spark.read.parquet(behavioral_data_path)

    # --- Data Preparation ---
    trans_df = trans_df.withColumn("InvoiceDate", col("InvoiceDate").cast(TimestampType()))
    
    # --- Feature Engineering ---
    current_timestamp = datetime.utcnow()

    # 1. RFM-T Features
    customer_summary = trans_df.groupBy("CustomerID").agg(
        max("InvoiceDate").alias("last_purchase_date"),
        min("InvoiceDate").alias("first_purchase_date"),
        countDistinct("InvoiceNo").alias("frequency"),
        sum("SalePrice").alias("total_spend")
    )

    rfm_t_features = customer_summary.withColumn("recency", datediff(col("last_purchase_date"), col("first_purchase_date"))) \
        .withColumn("T", datediff(lit(current_timestamp), col("first_purchase_date"))) \
        .withColumn("monetary", col("total_spend") / col("frequency")) \
        .select("CustomerID", "recency", "frequency", "monetary", "T")

    # 2. Time-Windowed Features (Transactional)
    window_spec_30d = window(col("InvoiceDate"), "30 days")
    window_spec_90d = window(col("InvoiceDate"), "90 days")

    time_window_features = trans_df.groupBy("CustomerID").agg(
        countDistinct("InvoiceNo", window_spec_30d).alias("purchase_count_30d"),
        sum(col("SalePrice"), window_spec_30d).alias("total_spend_30d"),
        countDistinct("InvoiceNo", window_spec_90d).alias("purchase_count_90d"),
        sum(col("SalePrice"), window_spec_90d).alias("total_spend_90d"),
    )
    
    # 3. Behavioral Features (Aggregating last 90 days of behavioral data)
    ninety_days_ago = current_timestamp - expr("INTERVAL 90 DAYS")
    behavioral_summary = behav_df.filter(col("event_timestamp") >= ninety_days_ago) \
        .groupBy("CustomerID").agg(
            countDistinct("session_id").alias("total_sessions_90d"),
            avg("session_duration_seconds").alias("avg_session_duration_90d"),
            sum(expr("case when event_type = 'add_to_cart' then 1 else 0 end")).alias("add_to_cart_count_90d")
        )

    # --- Join all features ---
    final_features = rfm_t_features \
        .join(time_window_features, "CustomerID", "left") \
        .join(behavioral_summary, "CustomerID", "left") \
        .na.fill(0) # Fill nulls from joins with 0
    
    # Add EventTime feature required by Feature Store
    final_features = final_features.withColumn("EventTime", lit(current_timestamp).cast(TimestampType()))

    return final_features

def write_to_feature_store(df, feature_group_name, region):
    """
    Ingests a Spark DataFrame into a SageMaker Feature Store.
    This function would typically use the SageMaker Feature Store SDK.
    For a Spark job, a common pattern is to convert to Pandas and use boto3.
    """
    # NOTE: In a high-scale scenario, you might use a more direct Spark-to-FeatureStore connector.
    # For simplicity, this demonstrates the boto3 approach within a Spark context.
    featurestore_client = get_sagemaker_feature_store_client(region)
    
    def put_records(partition):
        for row in partition:
            record = [
                {'FeatureName': name, 'ValueAsString': str(value)}
                for name, value in row.asDict().items()
            ]
            featurestore_client.put_record(
                FeatureGroupName=feature_group_name,
                Record=record
            )

    df.foreachPartition(put_records)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark-submit generate_features.py <transactional_data_path> <behavioral_data_path> <feature_group_name> <aws_region>")
        sys.exit(-1)

    spark = SparkSession.builder.appName("CLVFeatureGeneration").getOrCreate()

    transactional_data_path = sys.argv[1]
    behavioral_data_path = sys.argv[2]
    feature_group_name = sys.argv[3]
    aws_region = sys.argv[4]

    print(f"Starting feature generation...")
    features_df = generate_features(spark, transactional_data_path, behavioral_data_path)
    
    print(f"Writing {features_df.count()} records to Feature Group: {feature_group_name}")
    write_to_feature_store(features_df, feature_group_name, aws_region)
    
    print("Feature generation and ingestion complete.")
    spark.stop()