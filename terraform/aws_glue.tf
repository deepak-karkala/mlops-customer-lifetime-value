# terraform/aws_glue.tf

# IAM Role for the Glue job to access S3 and the DB connection
resource "aws_iam_role" "glue_job_role" {
  name = "clv-glue-job-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "glue.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess" # Scope down in production
}

resource "aws_iam_role_policy_attachment" "glue_basic_execution" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# The Glue job definition
resource "aws_glue_job" "ingest_transactional_job" {
  name     = "clv-ingest-transactional-job-${var.environment}"
  role_arn = aws_iam_role.glue_job_role.arn
  command {
    script_location = "s3://${var.artifacts_bucket_name}/scripts/ingest_transactional_data.py"
    python_version  = "3"
  }
  glue_version = "3.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

# The Glue connection to store DB credentials securely
resource "aws_glue_connection" "source_db_connection" {
  name = "clv-source-db-connection-${var.environment}"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://your-db-endpoint.rds.amazonaws.com:5432/ecommerce"
    USERNAME            = var.db_username
    PASSWORD            = var.db_password
  }
}