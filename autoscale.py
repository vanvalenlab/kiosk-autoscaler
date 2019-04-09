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
"""
Sort the key in the Redis base according to name and scale different
Kubernetes deployments as appropriate.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import logging
import time
import sys

import redis
import kubernetes

import autoscaler


def initialize_logger(debug_mode=True):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s]:[%(levelname)s]:[%(name)s]: %(message)s')
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)

    fh = logging.FileHandler('autoscaler.log')
    fh.setFormatter(formatter)

    if debug_mode:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(console)
    logger.addHandler(fh)


if __name__ == '__main__':
    initialize_logger()

    _logger = logging.getLogger(__file__)

    REDIS_CLIENT = redis.StrictRedis(
        host=os.getenv('REDIS_HOST'),
        port=os.getenv('REDIS_PORT'),
        decode_responses=True,
        charset='utf-8')

    kubernetes.config.load_incluster_config()

    KUBE_CLIENT = kubernetes.client.AppsV1Api()

    SCALER = autoscaler.Autoscaler(
        redis_client=REDIS_CLIENT,
        kube_client=KUBE_CLIENT,
        scaling_config=os.getenv('AUTOSCALING'),
        backoff_seconds=os.getenv('REDIS_INTERVAL', '1'))

    INTERVAL = int(os.getenv('INTERVAL', '5'))

    while True:
        try:
            SCALER.scale()
            _logger.debug('Sleeping for %s seconds.', INTERVAL)
            time.sleep(INTERVAL)
        except Exception as err:  # pylint: disable=broad-except
            _logger.critical('Fatal Error: %s: %s', type(err).__name__, err)
            sys.exit(1)
