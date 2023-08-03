# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

output "ecr_repository_url" {
  value = aws_ecr_repository.grove.repository_url
}
