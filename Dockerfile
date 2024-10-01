# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

FROM python:3.12-slim

# Copy in Grove ready for installation.
WORKDIR /tmp/grove
COPY grove grove/
COPY pyproject.toml .

# Install Grove from sources, and clean-up.
RUN pip install --no-cache-dir /tmp/grove && \
    rm -rf /tmp/grove

ENTRYPOINT ["grove"]
