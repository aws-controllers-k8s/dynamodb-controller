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

import json
import logging
import time
from typing import Dict, Tuple

import boto3
import pytest
from acktest import tags
from acktest.aws.identity import get_region, get_account_id
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import (CRD_GROUP, CRD_VERSION, condition, get_resource_tags,
                 load_dynamodb_resource, service_marker, table,
                 wait_for_cr_status)
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "tables"
CREATE_WAIT_AFTER_SECONDS = 30
DELETE_WAIT_AFTER_SECONDS = 15
MODIFY_WAIT_AFTER_SECONDS = 90

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

    time.sleep(CREATE_WAIT_AFTER_SECONDS)

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

@pytest.fixture(scope="function")
def table_basic():
    resource_name = random_suffix_name("table-basic", 32)
    (ref, cr) = create_table(resource_name, "table_basic")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@pytest.fixture(scope="function")
def table_insights():
    resource_name = random_suffix_name("table-insights", 32)
    (ref, cr) = create_table(resource_name, "table_insights")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@pytest.fixture(scope="module")
def table_basic_pay_per_request():
    resource_name = random_suffix_name("table-basic-pay-per-request", 32)
    (ref, cr) = create_table(resource_name, "table_basic_pay_per_request")

    yield ref, cr
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass


@pytest.fixture(scope="function")
def table_resource_policy():
    resource_name = random_suffix_name("table-resource-policy", 32)
    account_id = get_account_id()
    region = get_region()
    resource_policy = {
        "Version": "2012-10-17",
        "Id": "ack-table-with-policy",
        "Statement": [
            {
                "Sid": "EnableResourcePolicyOnTable",
                "Effect": "Allow",
                "Principal": {
                    "AWS": f'arn:aws:iam::{account_id}:root'
                },
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:Query",
                    "dynamodb:Scan"
                ],
                "Resource": f'arn:aws:dynamodb:{region}:{account_id}:table/{resource_name}'
            }
        ]
    }

    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = resource_name
    replacements["RESOURCE_POLICY"] = json.dumps(resource_policy)

    # Create the k8s resource
    resource_data = load_dynamodb_resource(
        "table_resource_policy",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, "tables",
        resource_name, namespace="default",
    )

    time.sleep(CREATE_WAIT_AFTER_SECONDS)

    # Create table
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    # Wait for the resource to be synced (table created and policy applied)
    # Resource policy is applied after table creation
    assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=5)

    # Now wait for table to be ACTIVE
    wait_for_cr_status(
        ref,
        "tableStatus",
        "ACTIVE",
        90,
        3,
    )

    yield (ref, cr, resource_policy)

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

    def table_insight_status(self, table_name: str, status: str) -> bool:
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

        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        table.wait_until(
            table_name,
            table.ttl_on_attribute_matches("ForumName"),
        )

        ttl = table.get_time_to_live(table_name)
        assert ttl is not None
        assert ttl["AttributeName"] == "ForumName"
        ttl_status = ttl["TimeToLiveStatus"]
        assert ttl_status in ("ENABLED", "ENABLING")

    def test_enable_point_in_time_recovery(self, table_lsi):
        (ref, res) = table_lsi

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Update PITR
        updates = {
            "spec": {
                "continuousBackups": {
                    "pointInTimeRecoveryEnabled": True
                }
            }
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, updates)

        table.wait_until(
            table_name,
            table.point_in_time_recovery_matches(True),
        )

        pitr_enabled = table.get_point_in_time_recovery_enabled(table_name)
        assert pitr_enabled is not None
        assert pitr_enabled

        # turn off pitr again and ensure it is disabled
        updates = {
            "spec": {
                "continuousBackups": {
                    "pointInTimeRecoveryEnabled": False
                }
            }
        }

        # Patch k8s resource
        k8s.patch_custom_resource(ref, updates)

        table.wait_until(
            table_name,
            table.point_in_time_recovery_matches(False),
        )

        pitr_enabled = table.get_point_in_time_recovery_enabled(table_name)
        assert pitr_enabled is not None
        assert not pitr_enabled

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

    def test_update_insights(self, table_insights):
        (ref, res) = table_insights

        table_name = res["spec"]["tableName"]
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=5)

        cr = k8s.get_resource(ref)

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)
        assert cr['spec']['contributorInsights'] == "ENABLED"
        assert self.table_insight_status(table_name, "ENABLED")

        # Set provisionedThroughput
        updates = {
            "spec": {
                "contributorInsights": "DISABLE"
            }
        }
        # Patch k8s resource
        k8s.patch_custom_resource(ref, updates)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=5)
        cr = k8s.get_resource(ref)
        assert cr['spec']['contributorInsights'] == "DISABLE"
        assert self.table_insight_status(table_name, "DISABLED")

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
        cr["spec"]["sseSpecification"] = {
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

        # encounter an issue when running E2E test locally, sometimes the gsi is updated,
        # but SSEDescription is still being updated, add 2mins to wait (Julian Chu)
        time.sleep(120)
        latestTable = table.get(table_name)
        logging.info("latestTable: %s", latestTable)
        assert latestTable["StreamSpecification"] is not None
        assert latestTable["StreamSpecification"]["StreamEnabled"]

        assert latestTable["SSEDescription"] is not None
        assert latestTable["SSEDescription"]["Status"] == "ENABLED"

        assert latestTable["ProvisionedThroughput"] is not None
        assert latestTable["ProvisionedThroughput"]["ReadCapacityUnits"] == 10
        assert latestTable["ProvisionedThroughput"]["WriteCapacityUnits"] == 10

    def test_create_gsi_pay_per_request(self, table_basic_pay_per_request):
        (ref, res) = table_basic_pay_per_request

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Creating two more GSIs
        cr["spec"]["attributeDefinitions"] = [
            {
                "attributeName": "Bill",
                "attributeType": "S"
            },
            {
                "attributeName": "Total",
                "attributeType": "S"
            },
            {
                "attributeName": "User",
                "attributeType": "S"
            },
        ]

        gsi = {
            "indexName": "bill-per-user",
            "keySchema": [
                {
                    "attributeName": "User",
                    "keyType": "HASH",
                },
                {
                    "attributeName": "Bill",
                    "keyType": "RANGE",
                }
            ],
            "projection": {
                "projectionType": "ALL",
            }
        }

        cr["spec"]['globalSecondaryIndexes'] = [
            gsi,
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)
        k8s.wait_resource_consumed_by_controller(ref)
        table.wait_until(
            table_name,
            table.gsi_matches(
                [
                    gsi
                ],
            ),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*40,
            interval_seconds=15,
        )

    def test_create_gsi_same_attributes(self, table_basic):
        (ref, res) = table_basic

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Creating two GSI using the same attributes
        gsi = {
            "indexName": "total-bill",
            "keySchema": [
                {
                    "attributeName": "Total",
                    "keyType": "HASH",
                },
                {
                    "attributeName": "Bill",
                    "keyType": "RANGE",
                }
            ],
            "projection": {
                "projectionType": "ALL",
            }
        }

        cr["spec"]['globalSecondaryIndexes'] = [
            gsi,
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)
        k8s.wait_resource_consumed_by_controller(ref)
        table.wait_until(
            table_name,
            table.gsi_matches(
                [
                    gsi
                ],
            ),
            timeout_seconds=MODIFY_WAIT_AFTER_SECONDS*40,
            interval_seconds=15,
        )

    def test_resource_policy(self, table_resource_policy):
        (ref, res, resource_policy) = table_resource_policy
        table_name = res["spec"]["tableName"]

        assert self.table_exists(table_name)

        # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetResourcePolicy.html
        # Need to wait and use an arn to query if the resource policy is added to the table
        condition.assert_synced(ref)
        cr = k8s.wait_resource_consumed_by_controller(ref)
        table_arn = cr["status"]["ackResourceMetadata"]["arn"]

        policy = table.get_resource_policy(table_arn)
        assert policy is not None
        assert 'ack-table-with-policy' in policy['Policy']

        # Update resource policy - add statement ID to verify update
        resource_policy['Id'] = 'updated-table-policy'
        updates = {
            "spec": {
                "resourcePolicy": json.dumps(resource_policy)
            }
        }

        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)

        # Verify policy was updated
        policy = table.get_resource_policy(table_arn)
        assert 'updated-table-policy' in policy['Policy']

        # Delete resource policy
        cr = k8s.wait_resource_consumed_by_controller(ref)
        cr["spec"]["resourcePolicy"] = None

        k8s.patch_custom_resource(ref, cr)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Verify policy was deleted
        deleted_policy = table.get_resource_policy(table_arn)
        assert deleted_policy is None
