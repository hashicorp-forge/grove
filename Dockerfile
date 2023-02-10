# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

FROM python:3.9-alpine

ARG GROVE_VERSION

RUN pip install --no-cache-dir grove==$GROVE_VERSION

ENTRYPOINT ["grove"]
