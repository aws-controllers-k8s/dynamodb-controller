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
MODIFY_WAIT_AFTER_SECONDS = 5

@pytest.fixture(scope="module")
def forum_table():
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
        30,
    )

    yield (table_reference, table_resource)

    _, deleted = k8s.delete_custom_resource(table_reference, period_length=DELETE_WAIT_AFTER_SECONDS)
    assert deleted

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

    def test_create_delete(self, dynamodb_client, forum_table):
        (ref, res) = forum_table

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(dynamodb_client, table_name)

        # Delete k8s resource
        _, deleted = k8s.delete_custom_resource(ref, period_length=DELETE_WAIT_AFTER_SECONDS)
        assert deleted is True

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        # Check DynamoDB Table doesn't exists
        assert not self.table_exists(dynamodb_client, table_name)

    def test_table_update_tags(self, dynamodb_client, forum_table):
        (ref, res) = forum_table

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(dynamodb_client, table_name)

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
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        table_tags = tag.clean(get_resource_tags(cr["status"]["ackResourceMetadata"]["arn"]))
        assert len(table_tags) == len(tags)
        assert table_tags[0]['Key'] == tags[0]['key']
        assert table_tags[0]['Value'] == tags[0]['value']

    def test_enable_ttl(self, dynamodb_client, forum_table):
        (ref, res) = forum_table

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(dynamodb_client, table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Update TTL
        updates = {
            "spec": {
                "timeToLive": {
                    "attributeName": "ForumName",
                    "enabled": True
                }
            }
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        ttl = dynamodb_client.describe_time_to_live(TableName=table_name)
        assert ttl["TimeToLiveDescription"]["AttributeName"] == "ForumName"
        assert (ttl["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLED" or
            ttl["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLING")