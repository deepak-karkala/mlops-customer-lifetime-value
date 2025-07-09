resource "aws_iam_role" "sagemaker_pipeline_execution_role" {
  name = "sagemaker-pipeline-execution-role"

  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "sagemaker.amazonaws.com"
        },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

# Granting broad SageMaker permissions for simplicity in this example.
# In a real production environment, you would scope this down significantly.
resource "aws_iam_role_policy_attachment" "sagemaker_full_access" {
  role       = aws_iam_role.sagemaker_pipeline_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

# Policy allowing access to the S3 bucket for artifacts and the Feature Store
resource "aws_iam_policy" "sagemaker_pipeline_custom_policy" {
  name        = "sagemaker-pipeline-s3-featurestore-policy"
  description = "Allows SageMaker pipelines to access project resources"

  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::clv-artifacts-bucket",
          "arn:aws:s3:::clv-artifacts-bucket/*"
        ]
      },
      {
        Effect   = "Allow",
        Action   = [
          "sagemaker:DescribeFeatureGroup",
          # Permissions for Athena query to get data from offline store
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "glue:GetTable"
        ],
        Resource = "*" # Scope down in production
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker_pipeline_custom_policy_attach" {
  role       = aws_iam_role.sagemaker_pipeline_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_pipeline_custom_policy.arn
}