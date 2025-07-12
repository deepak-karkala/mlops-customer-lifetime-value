# terraform/ab_testing.tf

resource "aws_sns_topic" "ab_test_notifications_topic" {
  name = "clv-ab-test-notifications"
  display_name = "Notifications for CLV A/B Test Readiness"
  
  tags = {
    Environment = var.environment
    Purpose     = "AB-Testing"
  }
}

# We will reuse the sagemaker_inference_role created previously.
# No new roles are needed if permissions are broad enough.