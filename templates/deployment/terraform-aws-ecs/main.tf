# Create SSM parameters for all Grove configuration documents.
resource "aws_ssm_parameter" "connector_documents" {
  for_each = fileset(path.module, "connectors/**/*.json")

  type  = "SecureString"
  value = file(each.value)
  name  = format("/grove/connectors/%s", trimprefix(trimsuffix(each.value, ".json"), "connectors/"))
}

# Deploy Grove into ECS Fargate.
module "grove" {
  depends_on = [aws_ssm_parameter.connector_documents]
  source     = "./modules/grove"
}
