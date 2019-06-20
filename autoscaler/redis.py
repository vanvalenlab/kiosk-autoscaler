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
"""Fault tolerant RedisClient wrapper class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import logging

import redis


class RedisClient(object):

    def __init__(self, host, port, backoff=1):
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.backoff = backoff
        self._redis = self._get_redis_client(host=host, port=port)

    def _get_redis_client(self, host, port):  # pylint: disable=R0201
        return redis.StrictRedis(
            host=host, port=port,
            decode_responses=True,
            charset='utf-8')

    def __getattr__(self, name):
        redis_function = getattr(self._redis, name)

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
                except redis.exceptions.ReadOnlyError as err:
                    self.logger.warning('Encountered `READONLYERROR: %s` '
                                        'Resetting connection and retrying '
                                        'in %s seconds.', err, self.backoff)
                    self._redis.connection_pool.reset()
                    time.sleep(self.backoff)
                except Exception as err:
                    self.logger.error('Unexpected %s: %s when calling `%s %s`.',
                                      type(err).__name__, err,
                                      str(name).upper(), ' '.join(values))
                    raise err

        return wrapper
