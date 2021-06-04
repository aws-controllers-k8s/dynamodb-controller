# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import boto3
import pytest
import time
import logging
from typing import Dict, Any
from pathlib import Path

from acktest.k8s import resource as k8s
from acktest.resources import load_resource_file

SERVICE_NAME = "dynamodb"
CRD_GROUP = "dynamodb.services.k8s.aws"
CRD_VERSION = "v1alpha1"

# PyTest marker for the current service
service_marker = pytest.mark.service(arg=SERVICE_NAME)

bootstrap_directory = Path(__file__).parent
resource_directory = Path(__file__).parent / "resources"

def load_dynamodb_resource(resource_name: str, additional_replacements: Dict[str, Any] = {}):
    """ Overrides the default `load_resource_file` to access the specific resources
    directory for the current service.
    """
    return load_resource_file(resource_directory, resource_name, additional_replacements=additional_replacements)

def wait_for_cr_status(
    reference: k8s.CustomResourceReference,
    status_field: str,
    desired_status: str,
    wait_periods: int,
    period_length: int,
):
    """
    Waits for the specified condition in CR status to reach the desired value.
    """
    actual_status = None
    for _ in range(wait_periods):
        time.sleep(period_length)
        resource = k8s.get_resource(reference)
        actual_status = resource["status"][status_field]
        if actual_status == desired_status:
            break

    else:
        logging.error(
            f"Wait for status: {desired_status} timed out. Actual status: {actual_status}"
        )

    assert actual_status == desired_status