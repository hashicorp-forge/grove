# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

FROM python:3.12-slim

# Allow build-time specification of version.
ARG VERSION

# Keep things friendly.
LABEL org.opencontainers.image.title="Grove"
LABEL org.opencontainers.image.description="A Software as a Service (SaaS) log collection framework."
LABEL org.opencontainers.image.url="https://github.com/hashicorp-forge/grove"
LABEL org.opencontainers.image.version=$VERSION

# Copy in Grove ready for installation.
WORKDIR /tmp/grove
COPY grove grove/
COPY pyproject.toml .

# Install Grove from sources, and clean-up.
RUN pip install --no-cache-dir /tmp/grove && \
    rm -rf /tmp/grove

ENTRYPOINT ["grove"]
