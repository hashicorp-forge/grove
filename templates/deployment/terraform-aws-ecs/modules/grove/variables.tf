# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

variable "name" {
  type        = string
  description = "The name of the deployment"
  default     = "grove"
}

variable "container_image_tag" {
  type        = string
  description = "Grove tag associated with the docker image to deploy."
  default     = "latest"
}

variable "output_bucket_name" {
  type        = string
  description = "The name of the S3 bucket to create for logs to be output to."
}

variable "cpu" {
  type        = string
  default     = 256
  description = "The maximum amount of cpu, for the ecs task."
}

variable "memory" {
  type        = string
  default     = 512
  description = "The maximum amount of memory, in megabytes, for the ecs task."
}

variable "log_level" {
  type        = string
  default     = "INFO"
  description = "The log level of the function"

  validation {
    error_message = "The log level must be a valid Python logging name."

    # Taken from https://docs.python.org/3/library/logging.html#logging-levels
    condition = contains(
      [
        "CRITICAL",
        "ERROR",
        "WARNING",
        "INFO",
        "DEBUG",
      ],
      var.log_level,
    )
  }
}

variable "schedule" {
  type        = string
  description = "The CloudWatch schedule to invoke the Grove on"
  default     = "rate(10 minutes)"
}

variable "log_retention_in_days" {
  type        = number
  default     = 14
  description = "Number of days to retain logs in CloudWatch Logs"
}

