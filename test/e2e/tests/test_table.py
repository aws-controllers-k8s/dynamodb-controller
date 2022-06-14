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

"""Integration tests for the DynamoDB Table API.
"""

import boto3
import pytest
import time
import logging
from typing import Dict, Tuple

from acktest.resources import random_suffix_name
from acktest.k8s import resource as k8s
from e2e import (
    service_marker, CRD_GROUP, CRD_VERSION,
    load_dynamodb_resource, wait_for_cr_status, 
    get_resource_tags,
)
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import tag

RESOURCE_PLURAL = "tables"

DELETE_WAIT_AFTER_SECONDS = 15
UPDATE_TAGS_WAIT_AFTER_SECONDS = 5

@pytest.fixture(scope="module")
def dynamodb_client():
    return boto3.client("dynamodb")

@service_marker
@pytest.mark.canary
class TestTable:
    def get_table(self, dynamodb_client, table_name: str) -> dict:
        try:
            resp = dynamodb_client.describe_table(
                TableName=table_name,
            )
            return resp["Table"]

        except Exception as e:
            logging.debug(e)
            return None

    def table_exists(self, dynamodb_client, table_name: str) -> bool:
        return self.get_table(dynamodb_client, table_name) is not None

    def test_create_delete(self, dynamodb_client):
        resource_name = random_suffix_name("table", 32)

        replacements = REPLACEMENT_VALUES.copy()
        replacements["TABLE_NAME"] = resource_name

        # Load Table CR
        resource_data = load_dynamodb_resource(
            "table_forums",
            additional_replacements=replacements,
        )
        logging.debug(resource_data)

        # Create k8s resource
        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            resource_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        assert cr is not None
        assert k8s.get_resource_exists(ref)

        wait_for_cr_status(
            ref,
            "tableStatus",
            "ACTIVE",
            10,
            5,
        )

        # Check DynamoDB Table exists
        exists = self.table_exists(dynamodb_client, resource_name)
        assert exists

        # Delete k8s resource
        _, deleted = k8s.delete_custom_resource(ref)
        assert deleted is True

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        # Check DynamoDB Table doesn't exists
        exists = self.table_exists(dynamodb_client, resource_name)
        assert not exists

    def test_table_update_tags(self, dynamodb_client):
        resource_name = random_suffix_name("table", 32)

        replacements = REPLACEMENT_VALUES.copy()
        replacements["TABLE_NAME"] = resource_name

        # Load Table CR
        resource_data = load_dynamodb_resource(
            "table_forums",
            additional_replacements=replacements,
        )
        logging.debug(resource_data)

        # Create k8s resource
        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            resource_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        assert cr is not None
        assert k8s.get_resource_exists(ref)

        wait_for_cr_status(
            ref,
            "tableStatus",
            "ACTIVE",
            10,
            5,
        )

        # Check DynamoDB Table exists
        exists = self.table_exists(dynamodb_client, resource_name)
        assert exists

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Update table list of tags
        tags = [
            {
                "key": "key1",
                "value": "value1",
            },
        ]
        cr["spec"]["tags"] = tags

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)
        time.sleep(UPDATE_TAGS_WAIT_AFTER_SECONDS)

        table_tags = tag.clean(get_resource_tags(cr["status"]["ackResourceMetadata"]["arn"]))
        assert len(table_tags) == len(tags)
        assert table_tags[0]['Key'] == tags[0]['key']
        assert table_tags[0]['Value'] == tags[0]['value']

        # Delete k8s resource
        _, deleted = k8s.delete_custom_resource(ref)
        assert deleted is True

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        # Check DynamoDB Table doesn't exists
        exists = self.table_exists(dynamodb_client, resource_name)
        assert not exists