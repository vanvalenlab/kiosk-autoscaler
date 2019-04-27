# Copyright 2016-2019 The Van Valen Lab at the California Institute of
# Technology (Caltech), with support from the Paul Allen Family Foundation,
# Google, & National Institutes of Health (NIH) under Grant U24CA224309-01.
# All rights reserved.
#
# Licensed under a modified Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.github.com/vanvalenlab/kiosk-autoscaler/LICENSE
#
# The Work provided may be used for non-commercial academic purposes only.
# For any other use of the Work, including commercial use, please contact:
# vanvalenlab@gmail.com
#
# Neither the name of Caltech nor the names of its contributors may be used
# to endorse or promote products derived from this software without specific
# prior written permission.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================
"""Tests for Redis client wrapper class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random

import redis
import pytest

import autoscaler


class DummyRedis(object):
    def __init__(self, fail_tolerance=0, hard_fail=False):
        self.fail_count = 0
        self.fail_tolerance = fail_tolerance
        self.hard_fail = hard_fail

    def get_fail_count(self):
        if self.hard_fail:
            raise AssertionError('thrown on purpose')
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return self.fail_count


class TestRedis(object):

    def test_redis_client(self):  # pylint: disable=R0201
        fails = random.randint(1, 3)
        Redis = autoscaler.redis.RedisClient

        # monkey patch _get_redis_client function to use DummyRedis client
        def _get_redis_client(*args, **kwargs):  # pylint: disable=W0613
            return DummyRedis(fail_tolerance=fails)

        Redis._get_redis_client = _get_redis_client

        client = Redis(host='host', port='port', backoff=0)
        assert client.get_fail_count() == fails

        with pytest.raises(AttributeError):
            client.unknown_function()

        # test that other exceptions will raise.
        def _get_redis_client_bad(*args, **kwargs):  # pylint: disable=W0613
            return DummyRedis(fail_tolerance=fails, hard_fail=True)

        Redis._get_redis_client = _get_redis_client_bad

        client = Redis(host='host', port='port', backoff=0)
        with pytest.raises(AssertionError):
            client.get_fail_count()
