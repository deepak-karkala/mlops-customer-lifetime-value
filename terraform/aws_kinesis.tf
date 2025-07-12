# terraform/aws_kinesis.tf

resource "aws_kinesis_stream" "behavioral_events_stream" {
  name        = "clv-behavioral-events-stream-${var.environment}"
  shard_count = 1
}

resource "aws_kinesis_firehose_delivery_stream" "behavioral_stream_to_s3" {
  name        = "clv-behavioral-stream-to-s3-${var.environment}"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.raw_data_bucket.arn # Assuming raw_data_bucket is defined elsewhere
    
    # Buffer hints: deliver every 5 minutes or when 64MB is reached
    buffering_interval_in_seconds = 300
    buffering_size_in_mb          = 64

    # Convert incoming JSON to Parquet for efficiency
    data_format_conversion_configuration {
      enabled = true
      input_format_configuration {
        deserializer {
          hive_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
    }
  }

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.behavioral_events_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }
}

# Role for Firehose to read from Kinesis and write to S3
resource "aws_iam_role" "firehose_role" {
  name = "clv-firehose-role-${var.environment}"
  # Assume role policy allows firehose.amazonaws.com
}

# ... Attach necessary policies to firehose_role ...