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

import boto3
import pytest

DEFAULT_WAIT_UNTIL_TIMEOUT_SECONDS = 60*10
DEFAULT_WAIT_UNTIL_INTERVAL_SECONDS = 15

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
            pytest.fail("failed to match DBInstance before timeout")
        time.sleep(interval_seconds)


def get(table_name):
    """Returns a dict containing the Role record from the IAM API.

    If no such Table exists, returns None.
    """
    c = boto3.client('dynamodb')
    try:
        resp = c.describe_table(TableName=table_name)
        return resp['Table']
    except c.exceptions.ResourceNotFoundException:
        return None


def get_time_to_live(table_name):
    """Returns the TTL specification for the table with a supplied name.

    If no such Table exists, returns None.
    """
    c = boto3.client('dynamodb')
    try:
        resp = c.describe_time_to_live(TableName=table_name)
        return resp['TimeToLiveDescription']
    except c.exceptions.ResourceNotFoundException:
        return None
