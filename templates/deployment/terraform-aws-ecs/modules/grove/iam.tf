# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

data "aws_caller_identity" "current" {}

# Wire up required permissions for ECS to access other Grove resources in AWS.
data "aws_iam_policy_document" "ecs_trust_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name               = "${var.name}-ecs-task-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust_policy.json
}

resource "aws_iam_role_policy_attachment" "ecs-task-execution-role-policy-attachment" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task_role" {
  name               = "${var.name}-ecs-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust_policy.json
}

resource "aws_iam_role_policy" "ecs_task_policy" {
  role   = aws_iam_role.ecs_task_role.name
  name   = "grove-ecs-task-policy"
  policy = data.aws_iam_policy_document.ecs_permissions.json
}

data "aws_iam_policy_document" "ecs_permissions" {
  # SSM for encrypted parameters (secrets and configuration documents).
  statement {
    actions   = ["ssm:GetParameter", "ssm:GetParametersByPath"]
    resources = ["arn:aws:ssm:*:${data.aws_caller_identity.current.account_id}:parameter/grove/*"]
  }

  # CloudWatch metrics.
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }

  # CloudWatch logs.
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = [
      "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:log-group:/aws/ecs/${var.name}",
      "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:log-group:/aws/ecs/${var.name}:log-stream:*",
    ]
  }

  # Output to S3.
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/*"]
  }

  # Allow access to the Grove DynamoDB table.
  statement {
    actions = [
      "dynamodb:ConditionCheckItem",
      "dynamodb:GetItem",
      "dynamodb:DeleteItem",
      "dynamodb:UpdateItem",
      "dynamodb:PutItem",
      "dynamodb:GetRecords",
      "dynamodb:Query",
    ]
    resources = [
      aws_dynamodb_table.grove.arn
    ]
  }
}

