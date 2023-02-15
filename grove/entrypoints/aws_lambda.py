# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove AWS Lambda entrypoint."""

from typing import Any, Dict

from aws_lambda_powertools.utilities.typing import LambdaContext

from grove.entrypoints import base


def entrypoint(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    """Grove AWS Lambda wrapper.

    :param event: Unused event data for this AWS Lambda invocation.
    :param context: Execution context from AWS Lambda.

    :return: An unused status message.
    """
    base.entrypoint(
        context={
            "runtime": __file__,
            "runtime_id": context.aws_request_id,
            "lambda_function_arn": context.invoked_function_arn,
            "lambda_function_memory_size": context.memory_limit_in_mb,
            "lambda_request_id": context.aws_request_id,
        },
    )
    return {"Status": "OKAY"}


# Support local development if called as a script.
if __name__ == "__main__":
    # This breaks encapsulation, but the PowerTools provided helpers don't expose a way
    # to set properties - as they're designed to just be for type hints. We're not going
    # to extend the class only for testing, so here we are.
    context = LambdaContext()
    context._aws_request_id = "C0FFEEC0-FFEE-C0FF-EEC0-FFEEC0FFEEC0"
    context._memory_limit_in_mb = 128
    context._invoked_function_arn = "arn:aws:lambda:local:0123456789012:grove"

    entrypoint({}, context)
