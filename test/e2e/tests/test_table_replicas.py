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

"""Integration tests for the DynamoDB Table Replicas.
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

DELETE_WAIT_AFTER_SECONDS = 30
MODIFY_WAIT_AFTER_SECONDS = 180
REPLICA_WAIT_AFTER_SECONDS = 600

REPLICA_REGION_1 = "us-east-1"
REPLICA_REGION_2 = "eu-west-1"
REPLICA_REGION_3 = "eu-central-1"
REPLICA_REGION_4 = "ap-southeast-1"
REPLICA_REGION_5 = "eu-north-1"


def create_table_with_replicas(name: str, resource_template, regions=None):
    if regions is None:
        regions = [REPLICA_REGION_1, REPLICA_REGION_2]

    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = name
    replacements["REPLICA_REGION_1"] = regions[0]
    replacements["REPLICA_REGION_2"] = regions[1]

    resource_data = load_dynamodb_resource(
        resource_template,
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


@pytest.fixture(scope="module")
def table_with_replicas():
    table_name = random_suffix_name("table-replicas", 32)

    (ref, res) = create_table_with_replicas(
        table_name,
        "table_with_replicas",
        [REPLICA_REGION_1, REPLICA_REGION_2]
    )

    yield (ref, res)

    # Delete the k8s resource if it still exists
    if k8s.get_resource_exists(ref):
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)


def create_table_with_invalid_replicas(name: str):
    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = name
    replacements["REPLICA_REGION_1"] = REPLICA_REGION_1

    resource_data = load_dynamodb_resource(
        "table_with_replicas_invalid",
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
def table_with_invalid_replicas():
    table_name = random_suffix_name("table-invalid-replicas", 32)

    (ref, res) = create_table_with_invalid_replicas(table_name)

    yield (ref, res)

    # Delete the k8s resource if it still exists
    if k8s.get_resource_exists(ref):
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)


@service_marker
@pytest.mark.canary
class TestTableReplicas:
    def table_exists(self, table_name: str) -> bool:
        return table.get(table_name) is not None

    def test_create_table_with_replicas(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Wait for the table to be active
        table.wait_until(
            table_name,
            table.status_matches("ACTIVE"),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Wait for both initial replicas to be created
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_2]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Wait for both replicas to be active
        for region in [REPLICA_REGION_1, REPLICA_REGION_2]:
            table.wait_until(
                table_name,
                table.replica_status_matches(region, "ACTIVE"),
                timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
                interval_seconds=30,
            )

        # Verify the replica exists
        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 2
        region_names = [r["RegionName"] for r in replicas]
        assert REPLICA_REGION_1 in region_names
        assert REPLICA_REGION_2 in region_names
        for replica in replicas:
            assert replica["ReplicaStatus"] == "ACTIVE"

    def test_add_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Remove both initial replicas and add three new ones
        cr["spec"]["tableReplicas"] = [
            {"regionName": REPLICA_REGION_3},
            {"regionName": REPLICA_REGION_4},
            {"regionName": REPLICA_REGION_5}
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        # Wait for the replicas to be updated
        table.wait_until(
            table_name,
            table.replicas_match(
                [REPLICA_REGION_3, REPLICA_REGION_4, REPLICA_REGION_5]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Wait for all new replicas to be active
        for region in [REPLICA_REGION_3, REPLICA_REGION_4, REPLICA_REGION_5]:
            table.wait_until(
                table_name,
                table.replica_status_matches(region, "ACTIVE"),
                timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
                interval_seconds=30,
            )

        # Verify replicas
        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 3

        region_names = [r["RegionName"] for r in replicas]
        for region in [REPLICA_REGION_3, REPLICA_REGION_4, REPLICA_REGION_5]:
            assert region in region_names

        for replica in replicas:
            assert replica["ReplicaStatus"] == "ACTIVE"

    def test_remove_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Wait for the initial replicas to be created and be active
        table.wait_until(
            table_name,
            table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_2]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        for region in [REPLICA_REGION_1, REPLICA_REGION_2]:
            table.wait_until(
                table_name,
                table.replica_status_matches(region, "ACTIVE"),
                timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
                interval_seconds=30,
            )

        # Get CR latest revision
        cr = k8s.wait_resource_consumed_by_controller(ref)
        current_replicas = table.get_replicas(table_name)
        assert current_replicas is not None
        assert len(current_replicas) >= 1

        # Get the region names of the current replicas
        current_regions = [r["RegionName"] for r in current_replicas]
        logging.info(f"Current replicas: {current_regions}")

        # Keep all but the last replica
        regions_to_keep = current_regions[:-1]
        regions_to_remove = [current_regions[-1]]

        # Remove the last replica
        cr["spec"]["tableReplicas"] = [
            {"regionName": region} for region in regions_to_keep
        ]

        # Patch k8s resource
        k8s.patch_custom_resource(ref, cr)

        # Wait for the replica to be removed
        table.wait_until(
            table_name,
            table.replicas_match(regions_to_keep),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Verify remaining replicas
        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == len(regions_to_keep)

        # Check that removed region is gone and kept regions are present
        region_names = [r["RegionName"] for r in replicas]
        for region in regions_to_keep:
            assert region in region_names
        for region in regions_to_remove:
            assert region not in region_names

    def test_delete_table_with_replicas(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Delete the k8s resource
        k8s.delete_custom_resource(ref)

        max_wait_seconds = REPLICA_WAIT_AFTER_SECONDS
        interval_seconds = 30
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            if not self.table_exists(table_name):
                break
            time.sleep(interval_seconds)

        # Verify the table was deleted
        assert not self.table_exists(table_name)
        replicas = table.get_replicas(table_name)
        assert replicas is None

    def test_terminal_condition_for_invalid_stream_specification(self, table_with_invalid_replicas):
        (ref, res) = table_with_invalid_replicas

        table_name = res["spec"]["tableName"]

        # Check DynamoDB Table exists
        assert self.table_exists(table_name)

        # Wait for the terminal condition to be set
        max_wait_seconds = 120
        interval_seconds = 10
        start_time = time.time()
        terminal_condition_set = False

        while time.time() - start_time < max_wait_seconds:
            cr = k8s.get_resource(ref)
            if cr and "status" in cr and "conditions" in cr["status"]:
                for condition_obj in cr["status"]["conditions"]:
                    if condition_obj["type"] == "ACK.Terminal" and condition_obj["status"] == "True":
                        terminal_condition_set = True
                        # Verify the error message
                        assert "table must have DynamoDB Streams enabled with StreamViewType set to NEW_AND_OLD_IMAGES" in condition_obj[
                            "message"]
                        break

            if terminal_condition_set:
                break

            time.sleep(interval_seconds)

        assert terminal_condition_set, "Terminal condition was not set for invalid StreamSpecification"

    # def test_simultaneous_updates(self, table_with_replicas):
    #     (ref, res) = table_with_replicas

    #     table_name = res["spec"]["tableName"]

    #     # Check DynamoDB Table exists
    #     assert self.table_exists(table_name)

    #     # Wait for the initial replicas to be created and be active
    #     table.wait_until(
    #         table_name,
    #         table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_2]),
    #         timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
    #         interval_seconds=30,
    #     )

    #     for region in [REPLICA_REGION_1, REPLICA_REGION_2]:
    #         table.wait_until(
    #             table_name,
    #             table.replica_status_matches(region, "ACTIVE"),
    #             timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
    #             interval_seconds=30,
    #         )

    #     # Get CR latest revision
    #     cr = k8s.wait_resource_consumed_by_controller(ref)

    #     # Prepare simultaneous updates to multiple fields:
    #     # 1. Update tags
    #     # 2. Change replicas
    #     # 3. Add GSI

    #     # Add attribute definitions needed for GSI
    #     cr["spec"]["attributeDefinitions"] = [
    #         {"attributeName": "PK", "attributeType": "S"},
    #         {"attributeName": "SK", "attributeType": "S"},
    #         {"attributeName": "GSI1PK", "attributeType": "S"},
    #         {"attributeName": "GSI1SK", "attributeType": "S"}
    #     ]

    #     # Add a GSI
    #     cr["spec"]["globalSecondaryIndexes"] = [{
    #         "indexName": "GSI1",
    #         "keySchema": [
    #             {"attributeName": "GSI1PK", "keyType": "HASH"},
    #             {"attributeName": "GSI1SK", "keyType": "RANGE"}
    #         ],
    #         "projection": {
    #             "projectionType": "ALL"
    #         }
    #     }]

    #     # Update replicas - remove one and add a different one
    #     # Set directly rather than depending on current replicas
    #     cr["spec"]["tableReplicas"] = [
    #         {"regionName": REPLICA_REGION_1},  # Keep the first defined region
    #         {"regionName": REPLICA_REGION_3}   # Add a new region
    #     ]

    #     # Update tags
    #     cr["spec"]["tags"] = [
    #         {"key": "Environment", "value": "Test"},
    #         {"key": "Purpose", "value": "SimontemourTest"},
    #         {"key": "UpdatedAt", "value": time.strftime("%Y-%m-%d")}
    #     ]

    #     # Patch k8s resource with all changes at once
    #     k8s.patch_custom_resource(ref, cr)

    #     # Wait for GSI to be created (usually the slowest operation)
    #     table.wait_until(
    #         table_name,
    #         table.gsi_matches([{
    #             "indexName": "GSI1",
    #             "keySchema": [
    #                 {"attributeName": "GSI1PK", "keyType": "HASH"},
    #                 {"attributeName": "GSI1SK", "keyType": "RANGE"}
    #             ],
    #             "projection": {
    #                 "projectionType": "ALL"
    #             }
    #         }]),
    #         timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*3,
    #         interval_seconds=30,
    #     )

    #     # Wait for replicas to be updated
    #     expected_regions = [REPLICA_REGION_1, REPLICA_REGION_3]
    #     table.wait_until(
    #         table_name,
    #         table.replicas_match(expected_regions),
    #         timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*3,
    #         interval_seconds=30,
    #     )

    #     # Verify all changes were applied

    #     # Check tags
    #     table_tags = get_resource_tags(
    #         cr["status"]["ackResourceMetadata"]["arn"])
    #     tags.assert_ack_system_tags(tags=table_tags)

    #     expected_tags = {
    #         "Environment": "Test",
    #         "Purpose": "SimontemourTest",
    #         "UpdatedAt": time.strftime("%Y-%m-%d")
    #     }

    #     # Verify custom tags (ignoring ACK system tags)
    #     for key, value in expected_tags.items():
    #         assert key in table_tags
    #         assert table_tags[key] == value

    #     # Verify GSI
    #     table_info = table.get(table_name)
    #     assert "GlobalSecondaryIndexes" in table_info
    #     assert len(table_info["GlobalSecondaryIndexes"]) == 1
    #     assert table_info["GlobalSecondaryIndexes"][0]["IndexName"] == "GSI1"

    #     # Verify replicas
    #     replicas = table.get_replicas(table_name)
    #     assert replicas is not None
    #     assert len(replicas) == 2
    #     region_names = [r["RegionName"] for r in replicas]
    #     assert REPLICA_REGION_1 in region_names
    #     assert REPLICA_REGION_3 in region_names
    #     assert REPLICA_REGION_2 not in region_names

    #     # Verify all replicas are active
    #     for replica in replicas:
    #         assert replica["ReplicaStatus"] == "ACTIVE"
