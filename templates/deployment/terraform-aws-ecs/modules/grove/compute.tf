# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

data "aws_region" "current" {}

# Create a repository for container images.
resource "aws_ecr_repository" "grove" {
  name = "grove"
}

# Deploy an ECS Fargate cluster for Grove to run in.
resource "aws_ecs_cluster" "grove" {
  name = "${var.name}-cluster"

  configuration {
    execute_command_configuration {
      logging = "OVERRIDE"

      log_configuration {
        cloud_watch_log_group_name = aws_cloudwatch_log_group.grove.name
      }
    }
  }

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "grove" {
  family                   = var.name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "${var.name}-container"
      image     = "${aws_ecr_repository.grove.repository_url}:${var.container_image_tag}"
      essential = true

      # Configuration is set through environment variables.
      environment = [
        { name = "LOG_LEVEL", value = var.log_level },

        # Cache handler configuration.
        { name = "GROVE_CACHE_HANDLER", value = "aws_dynamodb" },
        { name = "GROVE_CACHE_AWS_DYNAMODB_TABLE_REGION", value = data.aws_region.current.name },

        # Output handler configuration.
        { name = "GROVE_OUTPUT_HANDLER", value = "aws_s3" },
        { name = "GROVE_OUTPUT_AWS_S3_BUCKET", value = aws_s3_bucket.logs.bucket },
        { name = "GROVE_OUTPUT_AWS_S3_BUCKET_REGION", value = data.aws_region.current.name },

        # Configuration handler configuration.
        { name = "GROVE_CONFIG_HANDLER", value = "aws_ssm" },
        { name = "GROVE_CONFIG_AWS_SSM_SSM_REGION", value = data.aws_region.current.name },
      ]

      # Used for operational logs from Fargate, NOT collected log data.
      logConfiguration = {
        logDriver = "awslogs"

        options = {
          "awslogs-create-group"  = "true"
          "awslogs-group"         = aws_cloudwatch_log_group.grove.name
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = "${var.name}-container"
        }
      }
    }
  ])
}
