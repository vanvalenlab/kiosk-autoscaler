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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import time
import timeit
import logging
import subprocess

import redis


class Autoscaler(object):  # pylint: disable=useless-object-inheritance
    """Read Redis and scale up k8s pods if required.

    Args:
        redis_client: Redis Client Connection object.
        scaling_config: string, joined lists of autoscaling configurations
        backoff_seconds: int, after a redis/subprocess error, sleep for this
            many seconds and retry the command.
        deployment_delim: string, character delimiting deployment configs.
        param_delim: string, character delimiting deployment config parameters.
    """

    def __init__(self, redis_client, scaling_config, backoff_seconds=1,
                 deployment_delim=';', param_delim='|'):
        self.redis_client = redis_client
        self.backoff_seconds = int(backoff_seconds)
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.completed_statuses = {'done', 'failed'}

        self.autoscaling_params = self._get_autoscaling_params(
            scaling_config=scaling_config.rstrip(),
            deployment_delim=deployment_delim,
            param_delim=param_delim)

        self.redis_keys = {
            'predict': 0,
            'train': 0
        }

        self.pod_keywords = {
            'deployment': 'desired',
            'job': 'Completions'
        }

        self.tf_serving_pods = 0

    def _get_autoscaling_params(self, scaling_config,
                                deployment_delim=';',
                                param_delim='|'):
        if deployment_delim == param_delim:
            raise ValueError('`deployment_delim` and `param_delim` must be '
                             'different. Got "{}" and "{}".'.format(
                                 deployment_delim, param_delim))

        return [x.split(param_delim)
                for x in scaling_config.split(deployment_delim)]

    def _make_kubectl_call(self, args):
        argstring = ' '.join(args)
        count = 0
        start = timeit.default_timer()
        while True:
            try:
                subprocess.run(args)
                self.logger.debug('Executed `%s` (%s retries) in %s seconds.',
                                  argstring, count,
                                  timeit.default_timer() - start)
                break
            except subprocess.CalledProcessError as err:
                # Error during subprocess.  Log it and retry after `backoff
                self.logger.warning('Encountered "%s: %s" during subprocess '
                                    'command: `%s`.  Retrying in %s seconds.',
                                    type(err).__name__, err, argstring,
                                    self.backoff_seconds)
                count += 1
                time.sleep(self.backoff_seconds)

    def _get_kubectl_output(self, args):
        argstring = ' '.join(args)
        count = 0
        start = timeit.default_timer()
        while True:
            try:
                kubectl_output = subprocess.check_output(args)
                kubectl_output = kubectl_output.decode('utf8')
                self.logger.debug('Executed `%s` (%s retries) in %s seconds.',
                                  argstring, count,
                                  timeit.default_timer() - start)
                break
            except subprocess.CalledProcessError as err:
                # Error during subprocess.  Log it and retry after `backoff`
                self.logger.warning('Encountered "%s: %s" during subprocess '
                                    'command: `%s`.  Retrying in %s seconds.',
                                    type(err).__name__, err, argstring,
                                    self.backoff_seconds)
                count += 1
                time.sleep(self.backoff_seconds)
        return kubectl_output

    def scan_iter(self, match=None):
        while True:
            try:
                response = self.redis_client.scan_iter(match=match)
                break
            except redis.exceptions.ConnectionError as err:
                self.logger.warning('Encountered %s: %s when calling SCAN. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err,
                                    self.backoff_seconds)
                time.sleep(self.backoff_seconds)
        return response

    def hget(self, rhash, key):
        while True:
            try:
                response = self.redis_client.hget(rhash, key)
                break
            except redis.exceptions.ConnectionError as err:
                self.logger.warning('Encountered %s: %s when calling HGET. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err,
                                    self.backoff_seconds)
                time.sleep(self.backoff_seconds)
        return response

    def tally_keys(self):
        start = timeit.default_timer()
        # reset the key tallies to 0
        for k in self.redis_keys:
            self.redis_keys[k] = 0

        for key in self.scan_iter():
            if any(re.match(k, key) for k in self.redis_keys):
                status = self.hget(key, 'status')

                # add up each type of key that is "in-progress" or "new"
                if status is not None and status not in self.completed_statuses:
                    for k in self.redis_keys:
                        if re.match(k, key):
                            self.redis_keys[k] += 1

        self.logger.debug('Finished tallying redis keys in %s seconds.',
                          timeit.default_timer() - start)
        self.logger.info('Tallied redis keys: %s', self.redis_keys)

    def get_current_pods(self, namespace, resource_type, deployment):
        """Find the number of current pods deployed for the given resource"""
        # pod_checking_keyword = self.pod_keywords.get(resource_type)
        if resource_type not in self.pod_keywords:
            raise ValueError('The resource_type of {} is unsuitable. Use either'
                             '`deployment` or `job`'.format(resource_type))

        deployment_re = r'Replicas:\s+([0-9]+) desired | [0-9]+ updated | ' + \
                        r'[0-9]+ total | [0-9]+ available | [0-9]+ unavailable'

        description = self._get_kubectl_output([
            'kubectl', '-n', namespace, 'describe', resource_type, deployment
        ])

        current_pods = 0
        # dstr = str(description)[2:-1].encode('utf-8').decode('unicode_escape')
        for line in description.splitlines():
            if resource_type == 'deployment':
                potential_match = re.match(deployment_re, line)
                if potential_match is not None:
                    current_pods = potential_match.group(1)
                    break

            elif resource_type == 'job':
                potential_match = re.match(r'Completions:\s+([0-9]+)', line)
                # This works so long as we don't delete Redis keys after
                # they've been procesed.
                # If we do start deleting keys (queue system), then we'll need
                # to also identify the "Succeeded" line and subtract that value
                # from Completions.
                if potential_match is not None:
                    current_pods = potential_match.group(1)
                    break

        return int(current_pods)

    def get_desired_pods(self, deployment, key, keys_per_pod, min_pods,
                         max_pods, current_pods):
        autoscaled_deployments = {
            'redis-consumer-deployment': 7,
            'zip-consumer-deployment': 1,
            'data-processing-deployment': 1}

        if deployment in autoscaled_deployments:
            tf_serving_pods = self.get_current_pods(
                'deepcell', 'deployment', 'tf-serving-deployment')
            new_tf_serving_pods = tf_serving_pods - self.tf_serving_pods
            self.tf_serving_pods = tf_serving_pods
            extra_pods = new_tf_serving_pods * \
                autoscaled_deployments[deployment]
            desired_pods = current_pods + extra_pods
        else:
            desired_pods = self.redis_keys[key] // keys_per_pod

            # set `desired_pods` to inside the max/min boundaries.
            if desired_pods > max_pods:
                desired_pods = max_pods
            elif desired_pods < min_pods:
                desired_pods = min_pods

            # To avoid removing currently running pods, wait until all
            # pods of the deployment are idle before scaling down.
            if 0 < desired_pods < current_pods:
                desired_pods = current_pods

        return desired_pods

    def scale_deployments(self):
        for entry in self.autoscaling_params:
            # entry schema: minPods maxPods keysPerPod namespace resource_type
            #               predict_or_train deployment
            try:
                min_pods = int(entry[0])
                max_pods = int(entry[1])
                keys_per_pod = int(entry[2])
                namespace = str(entry[3])
                resource_type = str(entry[4])
                predict_or_train = str(entry[5])
                deployment = str(entry[6])
            except (IndexError, ValueError):
                self.logger.error('Autoscaling entry %s is malformed.', entry)
                continue

            self.logger.debug('Scaling %s', deployment)

            current_pods = self.get_current_pods(
                namespace, resource_type, deployment)

            # compute desired pods for this deployment
            desired_pods = self.get_desired_pods(
                deployment, predict_or_train, keys_per_pod,
                min_pods, max_pods, current_pods)

            self.logger.debug('%s %s in namespace %s has a current state of %s'
                              ' pods and a desired state of %s pods.',
                              str(resource_type).capitalize(), deployment,
                              namespace, current_pods, desired_pods)

            if desired_pods == current_pods:
                continue  # no scaling action is required

            if resource_type == 'job':
                # TODO: Find a suitable method for scaling jobs
                self.logger.debug('Job scaling has been temporarily disabled.')
                continue

            elif resource_type == 'deployment':
                self._make_kubectl_call([
                    'kubectl', 'scale', '-n', namespace,
                    '--replicas={}'.format(desired_pods),
                    '{}/{}'.format(resource_type, deployment)
                ])
                self.logger.info('Successfully scaled %s from %s to %s pods.',
                                 deployment, current_pods, desired_pods)

    def scale(self):
        self.tally_keys()
        self.scale_deployments()
