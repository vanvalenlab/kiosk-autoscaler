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
"""Fault tolerant Redis client wrapper class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
import random

import redis


REDIS_READONLY_COMMANDS = {
    'publish',
    'sunion',
    'readonly',
    'exists',
    'hstrlen',
    'lindex',
    'scan',
    'ping',
    'ttl',
    'wait',
    'zscore',
    'zrevrangebylex',
    'sscan',
    'geohash',
    'getbit',
    'hkeys',
    'zrange',
    'llen',
    'auth',
    'zcard',
    'dbsize',
    'subscribe',
    'zrangebylex',
    'zlexcount',
    'mget',
    'getrange',
    'bitpos',
    'lrange',
    'discard',
    'asking',
    'client',
    'pfselftest',
    'unsubscribe',
    'zrank',
    'readwrite',
    'hget',
    'bitcount',
    'randomkey',
    'time',
    'zrevrank',
    'sinter',
    'dump',
    'strlen',
    'unwatch',
    'smembers',
    'georadius',
    'lastsave',
    'slowlog',
    'sismember',
    'hexists',
    'multi',
    'sdiff',
    'geopos',
    'hscan',
    'script',
    'keys',
    'hvals',
    'pfcount',
    'zscan',
    'echo',
    'command',
    'select',
    'zcount',
    'substr',
    'pttl',
    'hlen',
    'info',
    'scard',
    'geodist',
    'srandmember',
    'hgetall',
    'pubsub',
    'psubscribe',
    'zrevrange',
    'hmget',
    'object',
    'watch',
    'zrangebyscore',
    'get',
    'type',
    'zrevrangebyscore',
    'punsubscribe',
    'georadiusbymember',
}


class RedisClient(object):

    def __init__(self, host, port, backoff=1):
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.backoff = backoff
        self._sentinel = self._get_redis_client(host=host, port=port)
        self._redis_master = self._sentinel
        self._redis_slaves = [self._sentinel]
        self._update_masters_and_slaves()

    def _update_masters_and_slaves(self):
        try:
            self._sentinel.sentinel_masters()

            sentinel_masters = self._sentinel.sentinel_masters()

            for master_set in sentinel_masters:
                master = sentinel_masters[master_set]

                redis_master = self._get_redis_client(
                    master['ip'], master['port'])

                redis_slaves = []
                for slave in self._sentinel.sentinel_slaves(master_set):
                    redis_slave = self._get_redis_client(
                        slave['ip'], slave['port'])
                    redis_slaves.append(redis_slave)

                self._redis_slaves = redis_slaves
                self._redis_master = redis_master
        except redis.exceptions.ResponseError as err:
            self.logger.warning('Encountered Error: %s. Using sentinel as '
                                'primary redis client.', err)

    def _get_redis_client(self, host, port):  # pylint: disable=R0201
        return redis.StrictRedis(host=host, port=port,
                                 decode_responses=True,
                                 charset='utf-8')

    def __getattr__(self, name):
        if name in REDIS_READONLY_COMMANDS:
            redis_client = random.choice(self._redis_slaves)
        else:
            redis_client = self._redis_master

        redis_function = getattr(redis_client, name)

        def wrapper(*args, **kwargs):
            values = list(args) + list(kwargs.values())
            while True:
                try:
                    return redis_function(*args, **kwargs)
                except redis.exceptions.ConnectionError as err:
                    self.logger.warning('Encountered %s: %s when calling '
                                        '`%s %s`. Retrying in %s seconds.',
                                        type(err).__name__, err,
                                        str(name).upper(),
                                        ' '.join(values), self.backoff)
                    time.sleep(self.backoff)
                except Exception as err:
                    self.logger.error('Unexpected %s: %s when calling `%s %s`.',
                                      type(err).__name__, err,
                                      str(name).upper(), ' '.join(values))
                    raise err

        return wrapper
