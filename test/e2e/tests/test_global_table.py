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
import json
import time
import logging
from typing import Dict, Tuple

from acktest.resources import random_suffix_name
from acktest.k8s import resource as k8s
from acktest.aws.identity import get_region
from e2e import (
    service_marker,
    CRD_GROUP,
    CRD_VERSION,
    load_dynamodb_resource,
    wait_for_cr_status,
)
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "globaltables"

CREATE_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 30

@pytest.fixture(scope="module")
def dynamodb_table():
    resource_name = random_suffix_name("table", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = resource_name

    # load resource
    resource_data = load_dynamodb_resource(
        "table_forums",
        additional_replacements=replacements,
    )

    table_reference = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, "tables",
        resource_name, namespace="default",
    )

    # Create table
    k8s.create_custom_resource(table_reference, resource_data)
    table_resource = k8s.wait_resource_consumed_by_controller(table_reference)

    assert table_resource is not None
    assert k8s.get_resource_exists(table_reference)

    wait_for_cr_status(
        table_reference,
        "tableStatus",
        "ACTIVE",
        10,
        5,
    )

    yield (table_reference, table_resource)

    _, deleted = k8s.delete_custom_resource(table_reference)
    assert deleted

@service_marker
@pytest.mark.canary
class TestGlobalTable:
    
    def get_global_table(self, dynamodb_client, global_table_name: str) -> dict:
        try:
            resp = dynamodb_client.describe_global_table(
                GlobalTableName=global_table_name,
            )
            return resp["GlobalTableDescription"]

        except Exception as e:
            logging.debug(e)
            return None

    def global_table_exists(self, dynamodb_client, global_table_name: str) -> bool:
        return self.get_global_table(dynamodb_client, global_table_name) is not None

    def test_smoke(self, dynamodb_client, dynamodb_table):
        (_, table_resource) = dynamodb_table

        # Global Tables must have the same name as dynamodb Tables
        global_table_name = table_resource["spec"]["tableName"]

        replacements = REPLACEMENT_VALUES.copy()
        replacements["REGION_NAME"] = get_region()
        replacements["TABLE_NAME"] = global_table_name
        replacements["GLOBAL_TABLE_NAME"] = global_table_name

        # Load GLobal Table CR
        resource_data = load_dynamodb_resource(
            "global_table",
            additional_replacements=replacements,
        )
        logging.debug(resource_data)

        # Create k8s resource
        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            global_table_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        assert cr is not None
        assert k8s.get_resource_exists(ref)

        wait_for_cr_status(
            ref,
            "globalTableStatus",
            "ACTIVE",
            10,
            5,
        )

        # Check DynamoDB Global Table exists
        exists = self.global_table_exists(dynamodb_client, global_table_name)
        assert exists

        _, deleted = k8s.delete_custom_resource(ref)
        assert deleted is True        

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        exists = self.global_table_exists(dynamodb_client, global_table_name)
        assert not exists