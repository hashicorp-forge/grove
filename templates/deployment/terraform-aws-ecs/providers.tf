# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

terraform {
  required_providers {
    aws = "~> 4.0"
  }

  required_version = "~> 1.4.0"
}

provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "Grove"
      Environment = "Production"
      Owner       = "team@example.org"
    }
  }
}
