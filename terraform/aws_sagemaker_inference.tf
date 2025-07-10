# terraform/aws_sagemaker_inference.tf

resource "aws_iam_role" "sagemaker_inference_role" {
  name = "sagemaker-inference-execution-role"

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

# This policy grants the necessary permissions for the batch transform job
resource "aws_iam_policy" "sagemaker_inference_policy" {
  name        = "sagemaker-inference-policy"
  description = "Allows SageMaker to run batch inference jobs for the CLV project"

  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:ListBucket"
        ],
        Resource = [
            "arn:aws:s3:::clv-artifacts-bucket/*", # Access to model artifacts
            "arn:aws:s3:::clv-inference-data-bucket/*" # Access to input/output data
        ]
      },
      {
        Effect = "Allow",
        Action = [
            "sagemaker:GetRecord"
        ],
        Resource = [
            "arn:aws:sagemaker:eu-west-1:${data.aws_caller_identity.current.account_id}:feature-group/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:CreateLogGroup",
            "logs:DescribeLogStreams"
        ],
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sagemaker_inference_policy_attach" {
  role       = aws_iam_role.sagemaker_inference_role.name
  policy_arn = aws_iam_policy.sagemaker_inference_policy.arn
}