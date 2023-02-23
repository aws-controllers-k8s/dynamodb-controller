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
from acktest import tags
from e2e import (
    service_marker, CRD_GROUP, CRD_VERSION,
    load_dynamodb_resource, wait_for_cr_status, 
    get_resource_tags,
)
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import condition
from e2e import table

RESOURCE_PLURAL = "tables"

DELETE_WAIT_AFTER_SECONDS = 15
MODIFY_WAIT_AFTER_SECONDS = 30

def new_gsi_dict(index_name: str, hash_key: str, read_write_pt: int):
    return {
        "indexName": index_name,
        "keySchema": [
            {
                "attributeName": "OfficeName",
                "keyType": "HASH",
            },
            {
                "attributeName": hash_key,
                "keyType": "RANGE",
            }
        ],
        "projection": {
            "nonKeyAttributes": ["Test"],
            "projectionType": "INCLUDE",
        },
        "provisionedThroughput": {
            "readCapacityUnits": read_write_pt,
            "writeCapacityUnits": read_write_pt
        },
    }

def create_table(name: str, resource_template):
    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = name

    # load resource
    resource_data = load_dynamodb_resource(
        resource_template,
        additional_replacements=replacements,
    )

    table_reference = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, "tables",
        name, namespace="default",
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
        90,
        3,
    )

    return table_reference, table_resource

@pytest.fixture(scope="module")
def table_lsi():
    resource_name = random_suffix_name("table-lsi", 32)
    (ref, cr) = create_table(resource_name, "table_local_secondary_indexes")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@pytest.fixture(scope="module")
def table_gsi():
    resource_name = random_suffix_name("table-gsi", 32)
    (ref, cr) = create_table(resource_name, "table_global_secondary_indexes")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@pytest.fixture(scope="module")
def all_in_table():
    resource_name = random_suffix_name("table-all-fields", 32)
    (ref, cr) = create_table(resource_name, "table_all_fields")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@pytest.fixture(scope="module")
def table_basic():
    resource_name = random_suffix_name("table-basic", 32)
    (ref, cr) = create_table(resource_name, "table_basic")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@service_marker
@pytest.mark.canary
class TestTable:
    def table_exists(self, table_name: str) -> bool:
        return table.get(table_name) is not None

    def test_create_delete(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]
        condition.assert_synced(ref)

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

    def test_table_update_tags(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Update table list of tags
        new_tags = [
            {
                "key": "key1",
                "value": "value1",
            },
        ]
        cr["spec"]["tags"] = new_tags

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        table_tags = get_resource_tags(cr["status"]["ackResourceMetadata"]["arn"])
        tags.assert_ack_system_tags(
            tags=table_tags,
        )
        tags_dict = tags.to_dict(
            new_tags,
            key_member_name="key",
            value_member_name="value"
        )
        tags.assert_equal_without_ack_tags(
            expected=tags_dict,
            actual=table_tags,
        )


        cr = k8s.wait_resource_consumed_by_controller(ref)
        # make multiple updates at once
        new_tags = [
            {
                "key": "key1",
                "value": "value2",
            },
            {
                "key": "key2",
                "value": "value2",
            },
            {
                "key": "key3",
                "value": "value3",
            },
        ]
        cr["spec"]["tags"] = new_tags

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        table_tags = get_resource_tags(cr["status"]["ackResourceMetadata"]["arn"])
        tags.assert_ack_system_tags(
            tags=table_tags,
        )
        tags_dict = tags.to_dict(
            new_tags,
            key_member_name="key",
            value_member_name="value"
        )
        tags.assert_equal_without_ack_tags(
            expected=tags_dict,
            actual=table_tags,
        )

    def test_enable_ttl(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

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

        table.wait_until(
            table_name,
            table.ttl_on_attribute_matches("ForumName"),
        )

        ttl = table.get_time_to_live(table_name)
        assert ttl is not None
        assert ttl["AttributeName"] == "ForumName"
        ttl_status = ttl["TimeToLiveStatus"]
        assert ttl_status in ("ENABLED", "ENABLING")

    def test_enable_stream_specification(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Disable stream
        cr["spec"]["streamSpecification"] = {
            "streamEnabled": False,
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.stream_specification_matches(False),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS,
            interval_seconds=3,
        )

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Disable stream
        cr["spec"]["streamSpecification"] = {
            "streamEnabled": True,
            "streamViewType": "NEW_AND_OLD_IMAGES"
        }
        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.stream_specification_matches(True),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS,
            interval_seconds=3,
        )

    def test_update_billing_mode(self, table_basic):
        (ref, res) = table_basic

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Set billing mode to PROVISIONED
        cr["spec"] = {
            "billingMode": "PROVISIONED",
            "provisionedThroughput": {
                "readCapacityUnits": 5,
                "writeCapacityUnits": 5
            }
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.billing_mode_matcher("PROVISIONED"),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS,
            interval_seconds=3,
        )

        table.wait_until(
            table_name,
            table.provisioned_throughput_matcher(5, 5),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS,
            interval_seconds=3,
        )

        # Need more billing mode updates quota (only from PROVISIONED -> PAY_PER_REQUEST)
        """ # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Disable stream
        cr["spec"] = {
            "billingMode": "PAY_PER_REQUEST",
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.billing_mode_matcher("PAY_PER_REQUEST"),
            timeout_seconds=60*3,
            interval_seconds=5,
        ) """

    def test_update_provisioned_throughput(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Set provisionedThroughput
        cr["spec"]["provisionedThroughput"] = {
            "readCapacityUnits": 10,
            "writeCapacityUnits": 10
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.provisioned_throughput_matcher(10, 10),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS,
            interval_seconds=3,
        )

    def test_enable_sse_specification(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Set server side encryption
        cr["spec"]["sseSpecification"] = {
            "enabled": True,
            "sseType": "KMS"
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.sse_specification_matcher(True, "KMS"),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*4,
            interval_seconds=3,
        )

        time.sleep(MODIFY_WAIT_AFTER_SECONDS*2)

        cr = k8s.wait_resource_consumed_by_controller(ref)
        # Unset server side encryption
        cr["spec"]["sseSpecification"] = {
            "enabled": False,
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.sse_specification_matcher(False, ""),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*4,
            interval_seconds=3,
        )

    def test_update_class(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Set table class
        cr["spec"]["tableClass"] = "STANDARD_INFREQUENT_ACCESS"

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.class_matcher("STANDARD_INFREQUENT_ACCESS"),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*6,
            interval_seconds=3,
        )

        time.sleep(MODIFY_WAIT_AFTER_SECONDS*2)

        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Set table class
        cr["spec"]["tableClass"] = "STANDARD"
        

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.class_matcher("STANDARD"),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*6,
            interval_seconds=3,
        )

    def test_simple_create_gsi(self, table_gsi):
        (ref, res) = table_gsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        table.wait_until(
            table_name,
            table.gsi_matches([new_gsi_dict("office-per-city", "City", 5)]),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*20,
            interval_seconds=15,
        )

    def test_create_multi_gsi(self, table_gsi):
        (ref, res) = table_gsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Creating two more GSIs
        cr["spec"]["attributeDefinitions"] = [
            {
                "attributeName": "OfficeName",
                "attributeType": "S"
            },
            {
                "attributeName": "Rank",
                "attributeType": "S"
            },
            {
                "attributeName": "City",
                "attributeType": "S"
            },
            {
                "attributeName": "Country",
                "attributeType": "S"
            },
            {
                "attributeName": "State",
                "attributeType": "S"
            },
        ]
        cr["spec"]['globalSecondaryIndexes'] = [
            new_gsi_dict("office-per-city", "City", 5),
            new_gsi_dict("office-per-country", "Country", 5),
            new_gsi_dict("office-per-state", "State", 5),
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.gsi_matches(
                [
                    new_gsi_dict("office-per-city", "City", 5),
                    new_gsi_dict("office-per-country", "Country", 5),
                    new_gsi_dict("office-per-state", "State", 5),
                ],
            ),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*40,
            interval_seconds=15,
        )

    def test_create_delete_gsi(self, table_gsi):
        (ref, res) = table_gsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Creating two more GSIs
        # Delete office-per-city and add office-per-country
        cr["spec"]["attributeDefinitions"] = [
            {
                "attributeName": "OfficeName",
                "attributeType": "S"
            },
            {
                "attributeName": "Rank",
                "attributeType": "S"
            },
            {
                "attributeName": "Country",
                "attributeType": "S"
            },
        ]
        cr["spec"]["globalSecondaryIndexes"] = [
            # deleting offices-per-city GSI
            new_gsi_dict("office-per-country", "Country", 5),
        ]


        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.gsi_matches([new_gsi_dict("office-per-country", "Country", 5)]),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*40,
            interval_seconds=15,
        )

    def test_create_update_gsi(self, table_gsi):
        (ref, res) = table_gsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Creating two more GSIs
        # Delete office-per-city and add office-per-country
        cr["spec"]["attributeDefinitions"] = [
            {
                "attributeName": "OfficeName",
                "attributeType": "S"
            },
            {
                "attributeName": "Rank",
                "attributeType": "S"
            },
            {
                "attributeName": "Country",
                "attributeType": "S"
            },
        ]
        cr["spec"]["globalSecondaryIndexes"] = [
            new_gsi_dict("office-per-city", "City", 10), # update op
            new_gsi_dict("office-per-country", "Country", 5), # new gsi
        ]
    

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.gsi_matches([new_gsi_dict("office-per-city", "City", 10), new_gsi_dict("office-per-country", "Country", 5)]),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*40,
            interval_seconds=15,
        )

    def test_multi_updates(self, table_gsi):
        (ref, res) = table_gsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        cr["spec"]["attributeDefinitions"] = [
            {
                "attributeName": "OfficeName",
                "attributeType": "S"
            },
            {
                "attributeName": "Rank",
                "attributeType": "S"
            },
            {
                "attributeName": "City",
                "attributeType": "S"
            },
            {
                "attributeName": "Country",
                "attributeType": "S"
            },
        ]
        cr["spec"]["globalSecondaryIndexes"] = [
            new_gsi_dict("office-per-city", "City", 5),
            new_gsi_dict("office-per-country", "Country", 5), # new gsi
        ]
        cr["spec"]["provisionedThroughput"] = {
            "readCapacityUnits": 10,
            "writeCapacityUnits": 10
        }
        cr["spec"][ "sseSpecification"] = {
            "enabled": True,
            "sseType": "KMS"
        }
        cr["spec"]["streamSpecification"] = {
            "streamEnabled": True,
            "streamViewType": "NEW_AND_OLD_IMAGES"
        }
        # NOTE: There are some impossible combination with provisionThroughput, tableClass,
        # seeSpec, streamSpec etc...

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        table.wait_until(
            table_name,
            table.gsi_matches([new_gsi_dict("office-per-city", "City", 5), new_gsi_dict("office-per-country", "Country", 5)]),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*40,
            interval_seconds=15,
        )

        # GSI is the last element to get update in the code path... so we just wait for it
        # to know that all the fields got updated.

        latestTable = table.get(table_name)
        assert latestTable["StreamSpecification"] is not None
        assert latestTable["StreamSpecification"]["StreamEnabled"]

        assert latestTable["SSEDescription"] is not None
        assert latestTable["SSEDescription"]["Status"] == "ENABLED"

        assert latestTable["ProvisionedThroughput"] is not None
        assert latestTable["ProvisionedThroughput"]["ReadCapacityUnits"] == 10
        assert latestTable["ProvisionedThroughput"]["WriteCapacityUnits"] == 10