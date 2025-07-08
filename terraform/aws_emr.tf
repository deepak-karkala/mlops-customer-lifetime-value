# IAM Role for the EMR service itself to manage AWS resources
resource "aws_iam_role" "emr_service_role" {
  name = "emr-service-role"

  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_policy_attach" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

# IAM Role for the EC2 instances within the EMR cluster
resource "aws_iam_role" "emr_ec2_role" {
  name = "emr-ec2-instance-role"

  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "emr-ec2-instance-profile"
  role = aws_iam_role.emr_ec2_role.name
}

resource "aws_iam_role_policy_attachment" "emr_ec2_policy_attach" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Custom policy to allow EMR instances to access S3 and SageMaker Feature Store
resource "aws_iam_policy" "emr_custom_policy" {
  name        = "emr-s3-featurestore-access-policy"
  description = "Allows EMR to read from raw S3 buckets and write to SageMaker Feature Store"

  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::clv-raw-data-bucket",
          "arn:aws:s3:::clv-raw-data-bucket/*"
        ]
      },
      {
        Effect   = "Allow",
        Action   = [
            "sagemaker:PutRecord"
        ],
        Resource = [
          "arn:aws:sagemaker:eu-west-1:${data.aws_caller_identity.current.account_id}:feature-group/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_custom_policy_attach" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = aws_iam_policy.emr_custom_policy.arn
}