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
"""Autoscaler class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import time
import timeit
import logging

import redis
import kubernetes


class Autoscaler(object):  # pylint: disable=useless-object-inheritance
    """Read Redis and scale up k8s pods if required.

    Args:
        redis_client: Redis Client Connection object.
        kube_client: Kubernetes Python Client object.
        scaling_config: string, joined lists of autoscaling configurations
        backoff_seconds: int, after a redis/subprocess error, sleep for this
            many seconds and retry the command.
        deployment_delim: string, character delimiting deployment configs.
        param_delim: string, character delimiting deployment config parameters.
    """

    def __init__(self, redis_client, scaling_config,
                 backoff_seconds=1, deployment_delim=';', param_delim='|'):
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

    def _get_autoscaling_params(self, scaling_config,
                                deployment_delim=';',
                                param_delim='|'):
        if deployment_delim == param_delim:
            raise ValueError('`deployment_delim` and `param_delim` must be '
                             'different. Got "{}" and "{}".'.format(
                                 deployment_delim, param_delim))

        return [x.split(param_delim)
                for x in scaling_config.split(deployment_delim)]

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

    def get_apps_v1_client(self):
        """Returns Kubernetes API Client for AppsV1Api"""
        kubernetes.config.load_incluster_config()
        return kubernetes.client.AppsV1Api()

    def get_batch_v1_client(self):
        """Returns Kubernetes API Client for AppsV1Api"""
        kubernetes.config.load_incluster_config()
        return kubernetes.client.BatchV1Api()

    def list_namespaced_deployment(self, namespace):
        """Wrapper for `kubernetes.client.list_namespaced_deployment`"""
        try:
            kube_client = self.get_apps_v1_client()
            response = kube_client.list_namespaced_deployment(namespace)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `list_namespaced_deployment`: %s',
                              type(err).__name__, err)
            raise err
        return response.items

    def list_namespaced_job(self, namespace):
        """Wrapper for `kubernetes.client.list_namespaced_job`"""
        try:
            kube_client = self.get_batch_v1_client()
            response = kube_client.list_namespaced_job(namespace)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `list_namespaced_job`: %s',
                              type(err).__name__, err)
            raise err
        return response.items

    def patch_namespaced_deployment(self, name, namespace, body):
        """Wrapper for `kubernetes.client.patch_namespaced_deployment`"""
        try:
            kube_client = self.get_apps_v1_client()
            response = kube_client.patch_namespaced_deployment(
                name, namespace, body)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `patch_namespaced_deployment`: '
                              '%s', type(err).__name__, err)
            raise err
        return response

    def patch_namespaced_job(self, name, namespace, body):
        """Wrapper for `kubernetes.client.patch_namespaced_job`"""
        try:
            kube_client = self.get_batch_v1_client()
            response = kube_client.patch_namespaced_deployment(
                name, namespace, body)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `patch_namespaced_job`: %s',
                              type(err).__name__, err)
            raise err
        return response

    def get_current_pods(self, namespace, resource_type, deployment):
        """Find the number of current pods deployed for the given resource"""
        if resource_type not in self.pod_keywords:
            raise ValueError('The resource_type of {} is unsuitable. Use either'
                             '`deployment` or `job`'.format(resource_type))

        current_pods = 0
        if resource_type == 'deployment':
            deployments = self.list_namespaced_deployment(namespace)
            for d in deployments:
                if d.metadata.name == deployment:
                    current_pods = d.spec.replicas
                    break

        elif resource_type == 'job':
            jobs = self.list_namespaced_job(namespace)
            for j in jobs:
                if j.metadata.name == deployment:
                    current_pods = j.spec.parallelism  # TODO: is this right?
                    break

        return int(current_pods)

    def get_desired_pods(self, key, keys_per_pod, min_pods, max_pods, current_pods):
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

            self.logger.debug('Scaling `%s`', deployment)

            current_pods = self.get_current_pods(
                namespace, resource_type, deployment)

            desired_pods = self.get_desired_pods(
                predict_or_train, keys_per_pod,
                min_pods, max_pods, current_pods)

            self.logger.debug('%s %s in namespace %s has a current state of %s'
                              ' pods and a desired state of %s pods.',
                              str(resource_type).capitalize(), deployment,
                              namespace, current_pods, desired_pods)

            if desired_pods == current_pods:
                continue  # no scaling action is required

            if resource_type == 'job':
                # TODO: Find a suitable method for scaling jobs
                res = self.patch_namespaced_job(deployment, namespace, body)
                body = {'spec': {'parallelism': desired_pods}}

            elif resource_type == 'deployment':
                body = {'spec': {'replicas': desired_pods}}
                res = self.patch_namespaced_deployment(deployment, namespace, body)
                self.logger.info('Successfully scaled %s from %s to %s pods.',
                                 deployment, current_pods, desired_pods)

    def scale(self):
        self.tally_keys()
        self.scale_deployments()
