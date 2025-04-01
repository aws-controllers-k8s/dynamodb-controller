# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Utilities for working with Table resources"""

import datetime
import time
import typing
import logging

import boto3
import pytest

from acktest.aws.identity import get_region

DEFAULT_WAIT_UNTIL_TIMEOUT_SECONDS = 60
DEFAULT_WAIT_UNTIL_INTERVAL_SECONDS = 5

TableMatchFunc = typing.NewType(
    'TableMatchFunc',
    typing.Callable[[dict], bool],
)

class StatusMatcher:
    def __init__(self, status):
        self.match_on = status

    def __call__(self, record: dict) -> bool:
        return ('TableStatus' in record
                and record['TableStatus'] == self.match_on)


def status_matches(status: str) -> TableMatchFunc:
    return StatusMatcher(status)


class TTLAttributeMatcher:
    def __init__(self, attr_name):
        self.attr_name = attr_name

    def __call__(self, record: dict) -> bool:
        if 'TableStatus' in record and record['TableStatus'] != 'ACTIVE':
            return False
        table_name = record['TableName']
        # NOTE(jaypipes): The reason we have to do this craziness is because
        # DynamoDB's DescribeTimeToLive API is straight up bonkers. If you
        # update the TTL on a Table, the Table needs to transition to ACTIVE
        # status before DescribeTimeToLive will return a 200 and even after it
        # does, you need to wait additional time until the
        # TimeToLiveDescription response shape contains an AttributeName field
        # that matches what you set in your update call. The
        # TimeToLiveDescription field can be empty or can be a blank struct
        # with no fields in it for a long time after updating TTL on a Table...
        ttl = get_time_to_live(table_name)
        if ttl is not None:
            if 'AttributeName' in ttl:
                if ttl['AttributeName'] == self.attr_name:
                    return True
        return False


def ttl_on_attribute_matches(attr_name: str) -> TableMatchFunc:
    return TTLAttributeMatcher(attr_name)

class PITRMatcher:
    def __init__(self, enabled: bool):
        self.enabled = enabled

    def __call__(self, record: dict) -> bool:
        pitr_enabled = get_point_in_time_recovery_enabled(record['TableName'])
        if pitr_enabled is None:
            return False
        return pitr_enabled == self.enabled

def point_in_time_recovery_matches(enabled: bool) -> TableMatchFunc:
    return PITRMatcher(enabled)

class StreamSpecificationMatcher:
    def __init__(self, enabled: bool):
        self.enabled = enabled

    def __call__(self, record: dict) -> bool:
        if self.enabled:
            return ('StreamSpecification' in record
                    and record["StreamSpecification"]["StreamEnabled"] == self.enabled)
        return not 'StreamSpecification' in record


def stream_specification_matches(enabled: str) -> TableMatchFunc:
    return StreamSpecificationMatcher(enabled)


class BillingModeMatcher:
    def __init__(self, mode: str):
        self.mode = mode

    def __call__(self, record: dict) -> bool:
        return ('BillingModeSummary' in record
                and record["BillingModeSummary"]["BillingMode"] == self.mode)


def billing_mode_matcher(mode: str) -> TableMatchFunc:
    return BillingModeMatcher(mode)


class ProvisionedThroughputMatcher:
    def __init__(self, read_capacity_units: int, write_capacity_units: int):
        self.read_capacity_units = read_capacity_units
        self.write_capacity_units = write_capacity_units

    def __call__(self, record: dict) -> bool:
        return ('ProvisionedThroughput' in record
                and record["ProvisionedThroughput"]["ReadCapacityUnits"] == self.read_capacity_units
                and record["ProvisionedThroughput"]["WriteCapacityUnits"] == self.write_capacity_units
        )

def provisioned_throughput_matcher(read_capacity_units: int, write_capacity_units: int)-> TableMatchFunc:
    return ProvisionedThroughputMatcher(read_capacity_units, write_capacity_units)


class SSESpecificationMatcher:
    def __init__(self, enabled: int, type: int):
        self.enabled = enabled
        self.type = type

    def __call__(self, record: dict) -> bool:
        if self.enabled:
            return ('SSEDescription' in record
                and record["SSEDescription"]["Status"] == "ENABLED"
        )
        return (not 'SSEDescription' in record
            or record["SSEDescription"]["Status"] == "DISABLED"
        )

def sse_specification_matcher(read_capacity_units: int, write_capacity_units: int)-> TableMatchFunc:
    return SSESpecificationMatcher(read_capacity_units, write_capacity_units)


class ClassMatcher:
    def __init__(self, cls):
        self.match_on = cls

    def __call__(self, record: dict) -> bool:
        if self.match_on == "STANDARD_INFREQUENT_ACCESS":
            return ('TableClassSummary' in record
                    and record['TableClassSummary']["TableClass"] == self.match_on)
        return (not 'TableClassSummary' in record) or (not record['TableClassSummary']["TableClass"] == self.match_on)


def class_matcher(cls: str) -> TableMatchFunc:
    return ClassMatcher(cls)


class GSIMatcher:
    def __init__(self, gsis):
        self.match_on = gsis

    def __call__(self, record: dict) -> bool:
        gsi_key = "GlobalSecondaryIndexes"
        if len(self.match_on) == 0:
            return (gsi_key not in record) or len(record[gsi_key]) == 0

        # if GSI is still in creating status , it will not be present in the record
        if gsi_key not in record:
            return False

        awsGSIs = record[gsi_key]
        if len(self.match_on) != len(record[gsi_key]):
            return False

        for awsGSI in awsGSIs:
            found = False
            for gsi in self.match_on:
                if awsGSI["IndexName"] == gsi["indexName"]:
                    found = True
                    break
            if not found:
                return False
            if not awsGSI["IndexStatus"] in ["ACTIVE"]:
                return False

        return True


def gsi_matches(gsis) -> TableMatchFunc:
    return GSIMatcher(gsis)

def wait_until(
        table_name: str,
        match_fn: TableMatchFunc,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a Table with a supplied name is returned from the DynamoDB
    API and the matching functor returns True.

    Usage:
        from e2e.table import wait_until, status_matches

        wait_until(
            table_name,
            status_matches("ACTIVE"),
        )

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while not match_fn(get(table_name)):
        if datetime.datetime.now() >= timeout:
            pytest.fail("failed to match table before timeout")
        time.sleep(interval_seconds)


def get(table_name):
    """Returns a dict containing the Role record from the IAM API.

    If no such Table exists, returns None.
    """
    c = boto3.client('dynamodb', region_name=get_region())
    try:
        resp = c.describe_table(TableName=table_name)
        return resp['Table']
    except c.exceptions.ResourceNotFoundException:
        return None

def get_insights(table_name):
    """Returns a dict containing the Role record from the IAM API.

    If no such Table exists, returns None.
    """
    c = boto3.client('dynamodb', region_name=get_region())
    try:
        resp = c.describe_contributor_insights(TableName=table_name)
        return resp['ContributorInsightsStatus']
    except c.exceptions.ResourceNotFoundException:
        return None


def get_time_to_live(table_name):
    """Returns the TTL specification for the table with a supplied name.

    If no such Table exists, returns None.
    """
    c = boto3.client('dynamodb', region_name=get_region())
    try:
        resp = c.describe_time_to_live(TableName=table_name)
        return resp['TimeToLiveDescription']
    except c.exceptions.ResourceNotFoundException:
        return None

def get_point_in_time_recovery_enabled(table_name):
    """Returns whether point in time recovery is enabled for the table with a supplied name.

    If no such Table exists, returns None.
    """
    c = boto3.client('dynamodb', region_name=get_region())
    try:
        resp = c.describe_continuous_backups(TableName=table_name)
        return resp['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus'] == 'ENABLED'
    except c.exceptions.ResourceNotFoundException:
        return None


class ReplicaMatcher:
    def __init__(self, expected_regions):
        self.expected_regions = expected_regions

    def __call__(self, record: dict) -> bool:
        if 'Replicas' not in record:
            return False

        actual_regions = set()
        for replica in record['Replicas']:
            if 'RegionName' in replica:
                actual_regions.add(replica['RegionName'])

        return set(self.expected_regions) == actual_regions


def replicas_match(expected_regions) -> TableMatchFunc:
    return ReplicaMatcher(expected_regions)


class ReplicaStatusMatcher:
    def __init__(self, region, status):
        self.region = region
        self.status = status

    def __call__(self, record: dict) -> bool:
        if 'Replicas' not in record:
            return False

        for replica in record['Replicas']:
            if 'RegionName' in replica and replica['RegionName'] == self.region:
                return 'ReplicaStatus' in replica and replica['ReplicaStatus'] == self.status

        return False


def replica_status_matches(region, status) -> TableMatchFunc:
    return ReplicaStatusMatcher(region, status)


def get_replicas(table_name):
    """Returns the replicas for a DynamoDB table

    Args:
        table_name: the name of the DynamoDB table to get replicas for
    Returns:
        A list of replicas or None if the table doesn't exist
    """
    dynamodb = boto3.client('dynamodb')
    try:
        response = dynamodb.describe_table(TableName=table_name)
        if 'Table' in response and 'Replicas' in response['Table']:
            return response['Table']['Replicas']
        return []
    except dynamodb.exceptions.ResourceNotFoundException:
        return None
