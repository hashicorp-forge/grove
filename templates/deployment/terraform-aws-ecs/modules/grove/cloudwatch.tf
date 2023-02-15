# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

# CloudWatch Logs are used for operational logs from ECS fargate, not for collected
# logs.
resource "aws_cloudwatch_log_group" "grove" {
  name              = "/aws/ecs/${var.name}"
  retention_in_days = var.log_retention_in_days
}

# Configure the CloudWatch event rule to invoke the function on a schedule.
resource "aws_cloudwatch_event_rule" "scheduled_run" {
  name                = "${var.name}-run-schedule"
  schedule_expression = var.schedule
}

# Allow CloudWatch Events to schedule work into the Grove ECS cluster. This is used to
# execute Grove periodically using CloudWatch events as a cron-like trigger.
resource "aws_iam_role" "ecs_events" {
  name               = "ecs_events"
  assume_role_policy = data.aws_iam_policy_document.ecs_events.json
}

data "aws_iam_policy_document" "ecs_events" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "ecs_events_run_task_with_any_role" {
  name   = "ecs_events_run_task_with_any_role"
  role   = aws_iam_role.ecs_events.id
  policy = data.aws_iam_policy_document.ecs_events_run_task_with_any_role.json
}

data "aws_iam_policy_document" "ecs_events_run_task_with_any_role" {
  statement {
    actions   = ["iam:PassRole"]
    resources = ["*"]
  }

  statement {
    actions   = ["ecs:RunTask"]
    resources = [replace(aws_ecs_task_definition.grove.arn, "/:\\d+$/", ":*")]
  }
}

# Run Grove periodically using CloudWatch events, rather than a long lived container.
resource "aws_cloudwatch_event_target" "scheduled_run" {
  rule     = aws_cloudwatch_event_rule.scheduled_run.name
  arn      = aws_ecs_cluster.grove.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    launch_type         = "FARGATE"
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.grove.arn

    network_configuration {
      assign_public_ip = "false"
      subnets          = [aws_subnet.private.id]
    }
  }
}
