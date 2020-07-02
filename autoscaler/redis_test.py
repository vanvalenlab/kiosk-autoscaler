# Copyright 2016-2020 The Van Valen Lab at the California Institute of
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
import time

import fakeredis
import redis
import pytest

from autoscaler.redis import RedisClient


class WrappedFakeStrictRedis(fakeredis.FakeStrictRedis):
    """Wrapper around fakeredis instance to mock errors and sentinel mode."""

    def __init__(self, *_, **kwargs):
        super(WrappedFakeStrictRedis, self).__init__(decode_responses='utf8')
        self.should_fail = kwargs.get('should_fail', False)

    def sentinel_masters(self, *_, **__):
        return {'mymaster': {'ip': 'master', 'port': 6379}}

    def sentinel_slaves(self, *_, **__):
        n = random.randint(2, 5)
        return [{'ip': 'slave', 'port': 6379} for i in range(n)]

    def busy_error(self, *_, **__):
        if self.should_fail:
            self.should_fail = False
            raise redis.exceptions.ResponseError('BUSY SCRIPT KILL ERROR')
        return True

    def connect_error(self, *_, **__):
        if self.should_fail:
            self.should_fail = False
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return True

    def fail(self, *_, **__):
        raise redis.exceptions.ResponseError('thrown on purpose')


class TestRedis(object):
    # pylint: disable=R0201

    def test_successful_command(self, mocker):
        mocker.patch('redis.StrictRedis', WrappedFakeStrictRedis)
        mocker.patch('autoscaler.redis.RedisClient._update_masters_and_slaves')

        client = RedisClient(host='host', port='port', backoff=0)

        # send some test requests using fakeredis
        key = 'job_id'
        values = {'data': str(random.randint(0, 100))}

        client.hmset(key, values)  # Non-readonly
        new_values = client.hgetall(key)

        assert new_values == values

        # test attribute error is thrown if invalid command.
        with pytest.raises(AttributeError):
            client.unknown_function()

    def test__update_masters_and_slaves(self, mocker):
        mocker.patch('redis.StrictRedis', WrappedFakeStrictRedis)
        client = RedisClient(host='host', port='port', backoff=0)

        master = client._redis_master
        slaves = client._redis_slaves

        # new master is same class but different instance.
        assert isinstance(master, WrappedFakeStrictRedis)
        assert isinstance(master, type(client._sentinel))
        assert master is not client._sentinel

        # starts with 1 slave, but should be 2 - 4 from mocked update.
        assert len(slaves) > 1
        for slave in slaves:
            assert isinstance(slave, WrappedFakeStrictRedis)
            assert isinstance(slave, type(client._sentinel))
            assert slave is not client._sentinel

        # test response error does not raise
        def redis_response_error(*_, **__):
            raise redis.exceptions.ResponseError('thrown on purpose')

        mocker.patch('autoscaler.redis.RedisClient._get_redis_client',
                     redis_response_error)
        client._update_masters_and_slaves()

    def test_error_handling(self, mocker):
        mocker.patch('redis.StrictRedis',
                     lambda *_, **__: WrappedFakeStrictRedis(should_fail=True))
        mocker.patch('autoscaler.redis.RedisClient._update_masters_and_slaves')

        client = RedisClient(host='host', port='port', backoff=0)
        with pytest.raises(redis.exceptions.ResponseError):
            client.fail()

        # mocked up ConnectionError
        client = RedisClient(host='host', port='port', backoff=0)
        spy = mocker.spy(client, '_update_masters_and_slaves')
        response = client.connect_error()
        assert response
        spy.assert_called_once_with()

        # mocked up retry-able ResponseError
        client = RedisClient(host='host', port='port', backoff=0)
        spy = mocker.spy(time, 'sleep')
        response = client.busy_error()
        assert response
        spy.assert_called_once_with(client.backoff)
