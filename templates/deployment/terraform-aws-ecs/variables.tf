# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

variable "container_image_tag" {
  description = "The tag to use when deploying the Grove container image (ECR)."
  type        = string
  default     = "latest"
}

variable "output_bucket_name" {
  description = "The name of the S3 bucket to create for outputting logs to."
  type        = string
}
