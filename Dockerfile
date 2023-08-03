# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

FROM python:3.9-alpine

RUN pip install --no-cache-dir grove

ENTRYPOINT ["grove"]
