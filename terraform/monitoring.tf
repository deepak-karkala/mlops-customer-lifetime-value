# terraform/monitoring.tf

# --- SNS Topics for Notifications ---
resource "aws_sns_topic" "critical_alerts_topic" {
  name = "clv-critical-alerts"
  tags = { Environment = var.environment }
}

resource "aws_sns_topic" "medium_alerts_topic" {
  name = "clv-medium-alerts"
  tags = { Environment = var.environment }
}

# Assume subscriptions (e.g., to a PagerDuty endpoint or email) are configured separately

# --- CloudWatch Alarms for System Health ---
resource "aws_cloudwatch_metric_alarm" "training_pipeline_failures" {
  alarm_name          = "clv-${var.environment}-training-pipeline-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "FailedJobs"
  namespace           = "AWS/SageMaker"
  period              = "3600" # Check hourly
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alerts when a SageMaker training job fails."
  alarm_actions       = [aws_sns_topic.critical_alerts_topic.arn]

  dimensions = {
    TrainingJobName = "clv-training-pipeline-*"
  }
}