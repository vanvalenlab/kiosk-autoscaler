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
"""Turn on and off k8s resources based on items in the Redis queue."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gc
import logging
import logging.handlers
import sys
import time

import decouple

import autoscaler


def initialize_logger(debug_mode=True):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '[%(asctime)s]:[%(levelname)s]:[%(name)s]: %(message)s')

    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)

    fh = logging.handlers.RotatingFileHandler(
        filename='autoscaler.log',
        maxBytes=10000000,
        backupCount=10)
    fh.setFormatter(formatter)

    if debug_mode:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(console)
    logger.addHandler(fh)
    logging.getLogger('kubernetes.client.rest').setLevel(logging.INFO)


if __name__ == '__main__':
    initialize_logger()

    _logger = logging.getLogger(__file__)

    REDIS_CLIENT = autoscaler.redis.RedisClient(
        host=decouple.config('REDIS_HOST', cast=str, default='redis-master'),
        port=decouple.config('REDIS_PORT', default=6379, cast=int),
        backoff=decouple.config('REDIS_INTERVAL', default=1, cast=int))

    SCALER = autoscaler.Autoscaler(
        redis_client=REDIS_CLIENT,
        queues=decouple.config('QUEUES', default='predict,track', cast=str),
        queue_delim=decouple.config('QUEUE_DELIMITER', ',', cast=str))

    INTERVAL = decouple.config('INTERVAL', default=5, cast=int)

    RESOURCE_NAMESPACE = decouple.config('RESOURCE_NAMESPACE', default='default')
    RESOURCE_TYPE = decouple.config('RESOURCE_TYPE', default='deployment')
    RESOURCE_NAME = decouple.config('RESOURCE_NAME')

    MIN_PODS = decouple.config('MIN_PODS', default=0, cast=int)
    MAX_PODS = decouple.config('MAX_PODS', default=1, cast=int)
    KEYS_PER_POD = decouple.config('KEYS_PER_POD', default=1, cast=int)

    while True:
        try:
            SCALER.scale(namespace=RESOURCE_NAMESPACE,
                         resource_type=RESOURCE_TYPE,
                         name=RESOURCE_NAME,
                         min_pods=MIN_PODS,
                         max_pods=MAX_PODS,
                         keys_per_pod=KEYS_PER_POD)
            gc.collect()
            time.sleep(INTERVAL)
        except Exception as err:  # pylint: disable=broad-except
            _logger.critical('Fatal Error: %s: %s', type(err).__name__, err)
            sys.exit(1)
