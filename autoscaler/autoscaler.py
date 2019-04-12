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

    def __init__(self, redis_client, scaling_config, secondary_scaling_config,
                 backoff_seconds=1, deployment_delim=';', param_delim='|'):
        self.redis_client = redis_client
        self.backoff_seconds = int(backoff_seconds)
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.completed_statuses = {'done', 'failed'}

        self.autoscaling_params = self._get_autoscaling_params(
            scaling_config=scaling_config.rstrip(),
            deployment_delim=deployment_delim,
            param_delim=param_delim)

        self.autoscaled_deployments = self._get_secondary_autoscaling_params(
            secondary_scaling_config=secondary_scaling_config.rstrip(),
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

        self.previous_reference_pod_counts = {}

    def _get_autoscaling_params(self, scaling_config,
                                deployment_delim=';',
                                param_delim='|'):
        if deployment_delim == param_delim:
            raise ValueError('`deployment_delim` and `param_delim` must be '
                             'different. Got "{}" and "{}".'.format(
                                 deployment_delim, param_delim))

        return [x.split(param_delim)
                for x in scaling_config.split(deployment_delim)]

    def _get_secondary_autoscaling_params(self, secondary_scaling_config,
                                          deployment_delim=';',
                                          param_delim='|'):
        if deployment_delim == param_delim:
            raise ValueError('`deployment_delim` and `param_delim` must be '
                             'different. Got "{}" and "{}".'.format(
                                 deployment_delim, param_delim))

        secondary_autoscaling_params = [x.split(param_delim) for x in
                                        secondary_scaling_config.split(deployment_delim)]
        autoscaled_deployments = {}
        if len(secondary_autoscaling_params) > 1:
            for secondary_autoscaling in secondary_autoscaling_params:
                autoscaled_deployments[secondary_autoscaling[0]] = \
                    secondary_autoscaling[1]
        return autoscaled_deployments

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

    def get_current_pods(self, resource_namespace, resource_type, resource_name,
                         only_running=False):
        """Find the number of current pods deployed for the given resource"""
        # pod_checking_keyword = self.pod_keywords.get(resource_type)
        if resource_type not in self.pod_keywords:
            raise ValueError('The resource_type of {} is unsuitable. Use either'
                             '`deployment` or `job`'.format(resource_type))

        resource_name_re = r'Replicas:\s+([0-9]+) desired \| [0-9]+ updated \| ' + \
                        r'[0-9]+ total \| ([0-9]+) available \| [0-9]+ unavailable'

        description = self._get_kubectl_output([
            'kubectl', '-n', resource_namespace, 'describe',
            resource_type, resource_name
        ])

        current_pods = 0
        # dstr = str(description)[2:-1].encode('utf-8').decode('unicode_escape')
        for line in description.splitlines():
            if resource_type == 'deployment':
                potential_match = re.match(resource_name_re, line)
                if potential_match is not None:
                    if only_running:
                        current_pods = potential_match.group(2)
                    else:
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

    def get_desired_pods(self, resource_name, key, keys_per_pod, min_pods,
                         max_pods, current_pods):
        if resource_name in self.autoscaled_deployments:
            extra_pods = self.new_tf_serving_pods * \
                autoscaled_deployments[resource_name]
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

    def secondary_desired_pods(self, resource_name, reference_pods,
            pods_per_reference_pod, min_pods, max_pods, current_pods):
        desired_pods = current_pods + pods_per_reference_pod*reference_pods

        # trim `desired_pods` to lay inside the max/min boundaries.
        if desired_pods > max_pods:
            desired_pods = max_pods
        elif desired_pods < min_pods:
            desired_pods = min_pods

        # To avoid removing currently running pods, wait until all
        # pods of the deployment are idle before scaling down.
        if 0 < desired_pods < current_pods:
            desired_pods = current_pods

        return desired_pods

    def scale_resource(desired_pods, current_pods, resource_type,
                       resource_namespace, resource_name):
        if desired_pods == current_pods:
            continue  # no scaling action is required
        if resource_type == 'job':
            # TODO: Find a suitable method for scaling jobs
            self.logger.debug('Job scaling has been temporarily disabled.')
            continue
        elif resource_type == 'deployment':
            self._make_kubectl_call([
                'kubectl', 'scale', '-n', resource_namespace,
                '--replicas={}'.format(desired_pods),
                '{}/{}'.format(resource_type, resource_name)
            ])
            self.logger.info('Successfully scaled %s from %s to %s pods.',
                             resource_name, current_pods, desired_pods)

    def scale_all_resources(self):
        for entry in self.autoscaling_params:
            # entry schema: minPods maxPods keysPerPod namespace resource_type
            #               predict_or_train deployment
            try:
                min_pods = int(entry[0])
                max_pods = int(entry[1])
                keys_per_pod = int(entry[2])
                resource_namespace = str(entry[3])
                resource_type = str(entry[4])
                predict_or_train = str(entry[5])
                resource_name = str(entry[6])
            except (IndexError, ValueError):
                self.logger.error('Autoscaling entry %s is malformed.', entry)
                continue

            self.logger.debug('Scaling %s', resource_name)

            # compute desired pods for this deployment
            current_pods = self.get_current_pods(
                resource_namespace, resource_type, resource_name)
            desired_pods = self.get_desired_pods(
                resource_name, predict_or_train, keys_per_pod,
                min_pods, max_pods, current_pods)

            self.logger.debug('%s %s in namespace %s has a current state of %s'
                              ' pods and a desired state of %s pods.',
                              str(resource_type).capitalize(), resource_name,
                              resource_namespace, current_pods, desired_pods)

            # scale pods
            self.scale_resource(desired_pods, current_pods, resource_type,
                                resource_namespace, resource_name):

        for entry in self.secondary_autoscaling_params:
            try:
                resource_name = str(entry[0])
                resource_type = str(entry[1])
                resource_namespace = str(entry[2])
                reference_resource_name = str(entry[3])
                reference_resource_type = str(entry[4])
                reference_resource_namespace = str(entry[5])
                pods_per_other_pod = int(entry[6])
                min_pods = int(entry[7])
                max_pods = int(entry[8])
            except (IndexError, ValueError):
                self.logger.error('Autoscaling entry %s is malformed.', entry)
                continue

            self.logger.debug('Secondary scaling for %s', deployment)

            # keep track of how many reference pods we're working with
            if not self.previous_reference_pod_counts[resource_name]:
                self.previous_reference_pod_counts[resource_name] = 0
            current_reference_pods = self.get_current_pods(
                reference_resource_namespace,
                reference_resource_type,
                reference_resource_namespace,
                only_running=True)
            new_reference_pods = current_reference_pods - \
                self.previous_reference_pod_counts[resource_name]
            self.previous_reference_pod_counts[resource_name] = \
                    new_reference_pods

            # compute desired pods for this deployment
            current_pods = self.get_current_pods(
                resource_namespace, resource_type, resource_name)
            desired_pods = self.secondary_desired_pods(self, resource_name,
                                                       reference_pods,
                                                       pods_per_reference_pod,
                                                       min_pods, max_pods,
                                                       current_pods)

            # scale pods
            self.scale_resource(desired_pods, current_pods, resource_type,
                                resource_namespace, resource_name):

    def scale(self):
        self.tally_keys()
        self.scale_all_resources()
