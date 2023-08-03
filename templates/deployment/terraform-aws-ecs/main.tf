# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

# Create SSM parameters for all Grove configuration documents.
resource "aws_ssm_parameter" "connector_documents" {
  for_each = fileset(path.module, "connectors/**/*.json")

  type  = "SecureString"
  value = file(each.value)
  name  = format("/grove/connectors/%s", trimprefix(trimsuffix(each.value, ".json"), "connectors/"))
}

# Deploy Grove into ECS Fargate.
module "grove" {
  source              = "./modules/grove"
  container_image_tag = var.container_image_tag
  output_bucket_name  = var.output_bucket_name
}
