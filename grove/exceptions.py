# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Exceptions used by Grove."""


class GroveException(Exception):
    """All exceptions should inherit from this to allow for hierarchical handling."""


class ConfigurationException(GroveException):
    """Indicates that a configuration related error has occurred."""


class ConnectorMissingException(GroveException):
    """Indicates that a requested connector was not found."""


class ConcurrencyException(GroveException):
    """Indicates that Grove may be running in another location concurrently."""


class NotImplementedException(GroveException):
    """Indicates that the requested functionality has not been implemented."""


class NotFoundException(GroveException):
    """Indicates that the requested entity was not found."""


class RequestFailedException(GroveException):
    """Indicates that an upstream request failed for an unhandled reason."""


class RateLimitException(GroveException):
    """Indicate that an upstream rate-limit was encountered."""


class AccessException(GroveException):
    """Indicates an issue occurred while attempting to access the requested resource."""


class DataFormatException(GroveException):
    """Indicates an issue occurred while attempting to process data."""


class ProcessorError(GroveException):
    """Indicates that an error occurred when setting up or calling a processor."""
