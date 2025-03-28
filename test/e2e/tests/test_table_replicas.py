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
from e2e import (CRD_GROUP, CRD_VERSION, condition,
                 load_dynamodb_resource, service_marker, table)
from e2e.replacement_values import REPLACEMENT_VALUES
from acktest.k8s import condition

RESOURCE_PLURAL = "tables"

DELETE_WAIT_AFTER_SECONDS = 30
MODIFY_WAIT_AFTER_SECONDS = 600
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


@pytest.fixture(scope="function")
def table_with_replicas():
    table_name = random_suffix_name("table-replicas", 32)

    (ref, res) = create_table_with_replicas(
        table_name,
        "table_with_replicas",
        [REPLICA_REGION_1, REPLICA_REGION_2]
    )

    # Wait for table to be ACTIVE before proceeding
    table.wait_until(
        table_name,
        table.status_matches("ACTIVE"),
        timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
        interval_seconds=30,
    )

    # Wait for initial replicas to be ACTIVE before yielding
    table.wait_until(
        table_name,
        table.replicas_match([REPLICA_REGION_1, REPLICA_REGION_2]),
        timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
        interval_seconds=30,
    )

    yield (ref, res)


    deleted = k8s.delete_custom_resource(ref)
    assert deleted

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


@pytest.fixture(scope="function")
def table_replicas_gsi():
    table_name = random_suffix_name("table-replicas-gsi", 32)
    replacements = REPLACEMENT_VALUES.copy()
    replacements["TABLE_NAME"] = table_name
    replacements["REPLICA_REGION_1"] = REPLICA_REGION_1
    replacements["REPLICA_REGION_2"] = REPLICA_REGION_2

    resource_data = load_dynamodb_resource(
        "table_with_gsi_and_replicas",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        table_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    table.wait_until(
        table_name,
        table.status_matches("ACTIVE"),
        timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
        interval_seconds=30,
    )

    yield (ref, cr)

    deleted = k8s.delete_custom_resource(ref)
    time.sleep(DELETE_WAIT_AFTER_SECONDS)
    assert deleted

@service_marker
@pytest.mark.canary
class TestTableReplicas:
    def table_exists(self, table_name: str) -> bool:
        return table.get(table_name) is not None

    def test_create_table_with_replicas(self, table_with_replicas):
        (_, res) = table_with_replicas
        table_name = res["spec"]["tableName"]

        # Table should already be ACTIVE from fixture
        assert table.get(table_name) is not None

        # Verify replicas exist and are ACTIVE
        for region in [REPLICA_REGION_1, REPLICA_REGION_2]:
            table.wait_until(
                table_name,
                table.replica_status_matches(region, "ACTIVE"),
                timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
                interval_seconds=30,
            )

    def test_add_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas
        table_name = res["spec"]["tableName"]

        assert table.get(table_name) is not None
        table.wait_until(
            table_name,
            table.status_matches("ACTIVE"),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Update replicas
        cr = k8s.get_resource(ref)
        cr["spec"]["tableReplicas"] = [
            {"regionName": REPLICA_REGION_3},
            {"regionName": REPLICA_REGION_4},
            {"regionName": REPLICA_REGION_5}
        ]
        k8s.patch_custom_resource(ref, cr)
        table.wait_until(
            table_name,
            table.replicas_match(
                [REPLICA_REGION_3, REPLICA_REGION_4, REPLICA_REGION_5]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Verify all replicas are ACTIVE
        for region in [REPLICA_REGION_3, REPLICA_REGION_4, REPLICA_REGION_5]:
            table.wait_until(
                table_name,
                table.replica_status_matches(region, "ACTIVE"),
                timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
                interval_seconds=30,
            )

    def test_remove_replica(self, table_with_replicas):
        (ref, res) = table_with_replicas
        table_name = res["spec"]["tableName"]

        assert self.table_exists(table_name)

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

        cr = k8s.wait_resource_consumed_by_controller(ref)
        current_replicas = table.get_replicas(table_name)
        assert current_replicas is not None
        assert len(current_replicas) >= 1

        current_regions = [r["RegionName"] for r in current_replicas]
        logging.info(f"Current replicas: {current_regions}")

        regions_to_keep = current_regions[:-1]
        regions_to_remove = [current_regions[-1]]

        cr["spec"]["tableReplicas"] = [
            {"regionName": region} for region in regions_to_keep
        ]

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

        region_names = [r["RegionName"] for r in replicas]
        for region in regions_to_keep:
            assert region in region_names
        for region in regions_to_remove:
            assert region not in region_names

    def test_delete_table_with_replicas(self, table_with_replicas):
        (ref, res) = table_with_replicas

        table_name = res["spec"]["tableName"]
        assert self.table_exists(table_name)

    def test_terminal_condition_for_invalid_stream_specification(self, table_with_invalid_replicas):
        (ref, res) = table_with_invalid_replicas

        table_name = res["spec"]["tableName"]
        assert self.table_exists(table_name)

        max_wait_seconds = 120
        interval_seconds = 10
        start_time = time.time()
        terminal_condition_found = False

        while time.time() - start_time < max_wait_seconds:
            try:
                condition.assert_type_status(
                    ref,
                    condition.CONDITION_TYPE_TERMINAL,
                    True)

                terminal_condition_found = True
                cond = k8s.get_resource_condition(
                    ref, condition.CONDITION_TYPE_TERMINAL)
                assert "table must have DynamoDB Streams enabled with StreamViewType set to NEW_AND_OLD_IMAGES" in cond[
                    "message"]
                break
            except:
                time.sleep(interval_seconds)

        assert terminal_condition_found, "Terminal condition was not set for invalid StreamSpecification"

    def test_staged_replicas_and_gsi_updates(self, table_replicas_gsi):
        (ref, cr) = table_replicas_gsi
        table_name = cr["spec"]["tableName"]
        max_wait_seconds = REPLICA_WAIT_AFTER_SECONDS
        interval_seconds = 30
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            if self.table_exists(table_name):
                break
            time.sleep(interval_seconds)
        assert self.table_exists(table_name)

        table.wait_until(
            table_name,
            table.gsi_matches([{
                "indexName": "GSI1",
                "keySchema": [
                    {"attributeName": "GSI1PK", "keyType": "HASH"},
                    {"attributeName": "GSI1SK", "keyType": "RANGE"}
                ],
                "projection": {
                    "projectionType": "ALL"
                }
            }]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
            interval_seconds=30,
        )

        # Step 2: Update - add second GSI and two more replicas
        cr = k8s.wait_resource_consumed_by_controller(ref)

        # Add attribute definition needed for GSI2
        cr["spec"]["attributeDefinitions"].append(
            {"attributeName": "GSI2PK", "attributeType": "S"}
        )

        # Add GSI2
        cr["spec"]["globalSecondaryIndexes"].append({
            "indexName": "GSI2",
            "keySchema": [
                {"attributeName": "GSI2PK", "keyType": "HASH"}
            ],
            "projection": {
                "projectionType": "KEYS_ONLY"
            }
        })

        # Add two more replicas
        cr["spec"]["tableReplicas"] = [
            {"regionName": REPLICA_REGION_1,
             "globalSecondaryIndexes": [{"indexName": "GSI1"}, {"indexName": "GSI2"}]},
            {"regionName": REPLICA_REGION_2,
             "globalSecondaryIndexes": [{"indexName": "GSI1"}, {"indexName": "GSI2"}]},
            {"regionName": REPLICA_REGION_3,
             "globalSecondaryIndexes": [{"indexName": "GSI1"}, {"indexName": "GSI2"}]}
        ]

        # Update the resource
        k8s.patch_custom_resource(ref, cr)

        # Wait for the new GSI to be created
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
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*2,
            interval_seconds=30,
        )

        table.wait_until(
            table_name,
            table.replicas_match(
                [REPLICA_REGION_1, REPLICA_REGION_2, REPLICA_REGION_3]),
            timeout_seconds=REPLICA_WAIT_AFTER_SECONDS*2,
            interval_seconds=30,
        )

        for region in [REPLICA_REGION_1, REPLICA_REGION_2, REPLICA_REGION_3]:
            table.wait_until(
                table_name,
                table.replica_status_matches(region, "ACTIVE"),
                timeout_seconds=REPLICA_WAIT_AFTER_SECONDS,
                interval_seconds=30,
            )

        table_info = table.get(table_name)
        assert "GlobalSecondaryIndexes" in table_info
        assert len(table_info["GlobalSecondaryIndexes"]) == 2
        gsi_names = [gsi["IndexName"]
                     for gsi in table_info["GlobalSecondaryIndexes"]]
        assert "GSI1" in gsi_names
        assert "GSI2" in gsi_names

        replicas = table.get_replicas(table_name)
        assert replicas is not None
        assert len(replicas) == 3
        region_names = [r["RegionName"] for r in replicas]
        assert REPLICA_REGION_1 in region_names
        assert REPLICA_REGION_2 in region_names
        assert REPLICA_REGION_3 in region_names
	