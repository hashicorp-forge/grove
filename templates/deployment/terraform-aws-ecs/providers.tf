# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

terraform {
  required_providers {
    aws = "~> 4.0"
  }
}

provider "aws" {
  region = "us-east-1"
}
