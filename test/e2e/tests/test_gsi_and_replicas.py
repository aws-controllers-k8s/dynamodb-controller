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

"""Integration tests for DynamoDB Tables with both GSIs and Replicas.
"""

import logging
import time
from typing import Dict, Tuple

import boto3
import pytest
from acktest import tags
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import (CRD_GROUP, CRD_VERSION, condition, get_resource_tags,
                 load_dynamodb_resource, service_marker, table,
                 wait_for_cr_status)
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "tables"

DELETE_WAIT_AFTER_SECONDS = 15
MODIFY_WAIT_AFTER_SECONDS = 90
REPLICA_WAIT_AFTER_SECONDS = 300  # Replicas can take longer to create/update

REPLICA_REGION_1 = "us-east-1"
REPLICA_REGION_2 = "eu-west-1"
REPLICA_REGION_3 = "eu-central-1"


def create_table_with_gsi_and_replicas(name: str):
    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = name
    replacements["REPLICA_REGION_1"] = REPLICA_REGION_1
    replacements["REPLICA_REGION_2"] = REPLICA_REGION_2

    resource_data = load_dynamodb_resource(
        "table_with_gsi_and_replicas",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    # Create the k8s resource
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    return (ref, cr)


@pytest.fixture(scope="function")
def table_with_gsi_and_replicas():
    table_name = random_suffix_name("table-gsi-replicas", 32)

    (ref, res) = create_table_with_gsi_and_replicas(table_name)

    yield (ref, res)

    # Delete the k8s resource if it still exists
    if k8s.get_resource_exists(ref):
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)


@service_marker
@pytest.mark.canary
class TestTableGSIAndReplicas:
    def table_exists(self, table_name: str) -> bool:
        return table.get(table_name) is not None

    def test_gsi_and_replicas_lifecycle(self, table_with_gsi_and_replicas):
        """Test full lifecycle of a table with both GSIs and replicas, including valid and invalid update scenarios:
        1. Create table with 2 GSIs and 2 replicas
        2. Verify GSIs and replicas are created correctly
        3. Test valid update: Update GSIs and replicas simultaneously
        4. Test invalid update: Try to add GSI only to parent but not replicas
        5. Remove all GSIs and replicas
        6. Delete table
        """
        (ref, res) = table_with_gsi_and_replicas
        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Wait for the table to be active
        table.wait_until(
            table_name,
            table.status_matches("ACTIVE"),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Step 1: Verify initial GSIs were created
        table.wait_until(
            table_name,
            table.gsi_matches([
                {
                    "indexName": "GSI1",
                    "keySchema": [
                        {"attributeName": "GSI1PK", "keyType": "HASH"},
                        {"attributeName": "GSI1SK", "keyType": "RANGE"}
                    ],
                    "projection": {
                        "projectionType": "ALL"
                    }
                },
                {
                    "indexName": "GSI2",
                    "keySchema": [
                        {"attributeName": "GSI2PK", "keyType": "HASH"}
                    ],
                    "projection": {
                        "projectionType": "KEYS_ONLY"
                    }
                }
            ]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Wait for both initial replicas to be created
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_2]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=15,
        )

        # Verify both GSIs and replicas are in correct state
        logging.info("Verifying initial GSIs and replicas...")
        table_info = table.get(table_name)
        assert "GlobalSecondaryIndexes" in table_info
        assert len(table_info["GlobalSecondaryIndexes"]) == 2

        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 2
        region_names = [r["RegionName"] for r in replicas]
        assert REPLICA_REGION_1 in region_names
        assert REPLICA_REGION_2 in region_names

        # Step 2: Valid update - Update GSIs and replicas simultaneously
        logging.info(
            "Performing valid update: Updating GSIs and replicas simultaneously...")
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Update attribute definitions to include a new GSI3 key
        cr["spec"]["attributeDefinitions"] = [
            {"attributeName": "PK", "attributeType": "S"},
            {"attributeName": "SK", "attributeType": "S"},
            {"attributeName": "GSI1PK", "attributeType": "S"},
            {"attributeName": "GSI1SK", "attributeType": "S"},
            {"attributeName": "GSI3PK", "attributeType": "S"},
        ]

        # Remove GSI2, keep GSI1, add GSI3
        cr["spec"]["globalSecondaryIndexes"] = [
            {
                "indexName": "GSI1",
                "keySchema": [
                    {"attributeName": "GSI1PK", "keyType": "HASH"},
                    {"attributeName": "GSI1SK", "keyType": "RANGE"}
                ],
                "projection": {
                    "projectionType": "ALL"
                }
            },
            {
                "indexName": "GSI3",
                "keySchema": [
                    {"attributeName": "GSI3PK", "keyType": "HASH"}
                ],
                "projection": {
                    "projectionType": "ALL"
                }
            }
        ]

        # Remove REPLICA_REGION_2, keep REPLICA_REGION_1, add REPLICA_REGION_3
        cr["spec"]["replicationGroup"] = [
            {"regionName": REPLICA_REGION_1},
            {"regionName": REPLICA_REGION_3}
        ]

        # Patch k8s resource with all changes at once
        k8s.patch_custom_resource(ref, cr)

        # Wait for GSIs to be updated
        table.wait_until(
            table_name,
            table.gsi_matches([
                {
                    "indexName": "GSI1",
                    "keySchema": [
                        {"attributeName": "GSI1PK", "keyType": "HASH"},
                        {"attributeName": "GSI1SK", "keyType": "RANGE"}
                    ],
                    "projection": {
                        "projectionType": "ALL"
                    }
                },
                {
                    "indexName": "GSI3",
                    "keySchema": [
                        {"attributeName": "GSI3PK", "keyType": "HASH"}
                    ],
                    "projection": {
                        "projectionType": "ALL"
                    }
                }
            ]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*2,
            interval_seconds=15,
        )

        # Wait for replicas to be updated
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_3]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*2,
            interval_seconds=15,
        )

        # Verify updates were applied correctly
        table_info = table.get(table_name)
        gsi_names = [gsi["IndexName"]
                     for gsi in table_info["GlobalSecondaryIndexes"]]
        assert "GSI1" in gsi_names
        assert "GSI3" in gsi_names
        assert "GSI2" not in gsi_names

        replicas = table.get_replicas(table_name)
        region_names = [r["RegionName"] for r in replicas]
        assert REPLICA_REGION_1 in region_names
        assert REPLICA_REGION_3 in region_names
        assert REPLICA_REGION_2 not in region_names

        # Step 3: Test invalid update - try to add GSI only to parent table
        logging.info(
            "Testing invalid update: Adding GSI only to parent table but not replicas...")
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Update attribute definitions to include a GSI that would only apply to parent
        cr["spec"]["attributeDefinitions"] = [
            {"attributeName": "PK", "attributeType": "S"},
            {"attributeName": "SK", "attributeType": "S"},
            {"attributeName": "GSI1PK", "attributeType": "S"},
            {"attributeName": "GSI1SK", "attributeType": "S"},
            {"attributeName": "GSI3PK", "attributeType": "S"},
            {"attributeName": "GSI4PK", "attributeType": "S"},
        ]

        # Add a new GSI4 and attempt to define it for parent table but not replicas
        cr["spec"]["globalSecondaryIndexes"] = [
            {
                "indexName": "GSI1",
                "keySchema": [
                    {"attributeName": "GSI1PK", "keyType": "HASH"},
                    {"attributeName": "GSI1SK", "keyType": "RANGE"}
                ],
                "projection": {
                    "projectionType": "ALL"
                }
            },
            {
                "indexName": "GSI3",
                "keySchema": [
                    {"attributeName": "GSI3PK", "keyType": "HASH"}
                ],
                "projection": {
                    "projectionType": "ALL"
                }
            },
            {
                "indexName": "GSI4",
                "keySchema": [
                    {"attributeName": "GSI4PK", "keyType": "HASH"}
                ],
                "projection": {
                    "projectionType": "ALL"
                },
                "replicaDefinitions": [
                    {
                        "regionName": REPLICA_REGION_1,
                        "propagateToReplica": False  # Not including this in replica
                    }
                ]
            }
        ]

        # Patch k8s resource with invalid configuration
        k8s.patch_custom_resource(ref, cr)

        # Wait for terminal condition to be set or timeout
        max_wait_seconds = 60
        interval_seconds = 5
        start_time = time.time()
        terminal_condition_set = False
        error_message_contains = "GSI must be created on all replicas"

        while time.time() - start_time < max_wait_seconds:
            cr = k8s.get_resource(ref)
            if cr and "status" in cr and "conditions" in cr["status"]:
                for condition_obj in cr["status"]["conditions"]:
                    if condition_obj["type"] == "ACK.Terminal" and condition_obj["status"] == "True":
                        if error_message_contains in condition_obj["message"]:
                            terminal_condition_set = True
                            logging.info(
                                f"Found terminal condition: {condition_obj['message']}")
                            break

            if terminal_condition_set:
                break
            time.sleep(interval_seconds)

        # Assert that we received a terminal condition for the invalid update
        assert terminal_condition_set, "Terminal condition was not set for invalid GSI configuration"

        # Get the latest resource state after invalid update failure
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Step 4: Remove all GSIs and replicas except the minimum required
        logging.info("Removing all GSIs and all but one replica...")

        # Update attribute definitions - keep only required for table
        cr["spec"]["attributeDefinitions"] = [
            {"attributeName": "PK", "attributeType": "S"},
            {"attributeName": "SK", "attributeType": "S"},
        ]

        # Remove all GSIs
        if "globalSecondaryIndexes" in cr["spec"]:
            cr["spec"].pop("globalSecondaryIndexes")

        # Keep only the primary region replica
        cr["spec"]["replicationGroup"] = [
            {"regionName": REPLICA_REGION_1}
        ]

        # Patch k8s resource with all changes at once
        k8s.patch_custom_resource(ref, cr)

        # Wait for all GSIs to be deleted
        max_wait_seconds = REPLICA_WAIT_AFTER_SECONDS*2
        interval_seconds = 15
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            table_info = table.get(table_name)
            if "GlobalSecondaryIndexes" not in table_info:
                logging.info("All GSIs have been deleted")
                break
            logging.info(
                f"Waiting for GSIs to be deleted, remaining: {len(table_info.get('GlobalSecondaryIndexes', []))}")
            time.sleep(interval_seconds)

        # Verify GSIs are deleted
        table_info = table.get(table_name)
        assert "GlobalSecondaryIndexes" not in table_info, "All GSIs should be deleted"

        # Wait for replicas to be updated
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*2,
            interval_seconds=15,
        )

        # Step 5: Delete the table
        logging.info("Deleting the table...")
        k8s.delete_custom_resource(ref)

        # Wait for the table to be deleted
        max_wait_seconds = REPLICA_WAIT_AFTER_SECONDS*2
        interval_seconds = 15
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            if not self.table_exists(table_name):
                break
            logging.info("Waiting for table to be deleted...")
            time.sleep(interval_seconds)

        # Verify table and all resources are deleted
        assert not self.table_exists(table_name)
        replicas = table.get_replicas(table_name)
        assert replicas is None
