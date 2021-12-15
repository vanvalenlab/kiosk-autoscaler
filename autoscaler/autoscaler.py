# Copyright 2016-2021 The Van Valen Lab at the California Institute of
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

import timeit
import logging

import kubernetes


class Autoscaler(object):
    """Read Redis and scale up k8s pods if required.

    Args:
        redis_client: Redis Client Connection object.
        scaling_config: string, joined lists of autoscaling configurations
        deployment_delim: string, character delimiting deployment configs.
        param_delim: string, character delimiting deployment config parameters.
    """

    def __init__(self,
                 redis_client,
                 queues='predict',
                 queue_delim=','):

        self.redis_keys = {q: 0 for q in queues.split(queue_delim)}

        self.redis_client = redis_client
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.completed_statuses = {'done', 'failed'}

        self.managed_resource_types = {'deployment', 'job'}

    def tally_queues(self):
        """Update counts of all redis queues"""
        start = timeit.default_timer()

        for q in self.redis_keys:
            self.logger.debug('Tallying items in queue `%s`.', q)

            num_items = self.redis_client.llen(q)

            processing_q = 'processing-{}:*'.format(q)
            scan = self.redis_client.scan_iter(match=processing_q, count=1000)
            num_in_progress = len([x for x in scan])  # pylint: disable=R1721

            self.redis_keys[q] = num_items + num_in_progress

        self.logger.debug('Finished tallying redis keys in %s seconds.',
                          timeit.default_timer() - start)
        self.logger.info('In-progress or new redis keys: %s', self.redis_keys)

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
        t = timeit.default_timer()
        try:
            kube_client = self.get_apps_v1_client()
            response = kube_client.list_namespaced_deployment(namespace)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `list_namespaced_deployment`: %s',
                              type(err).__name__, err)
            raise err
        self.logger.debug('Found %s deployments in namespace `%s` in '
                          '%s seconds.', len(response.items), namespace,
                          timeit.default_timer() - t)
        self.logger.debug('Specifically: %s',
                          [d.metadata.name for d in response.items])
        return response.items

    def list_namespaced_job(self, namespace):
        """Wrapper for `kubernetes.client.list_namespaced_job`"""
        t = timeit.default_timer()
        try:
            kube_client = self.get_batch_v1_client()
            response = kube_client.list_namespaced_job(namespace)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `list_namespaced_job`: %s',
                              type(err).__name__, err)
            raise err
        self.logger.debug('Found %s jobs in namespace `%s` in '
                          '%s seconds.', len(response.items), namespace,
                          timeit.default_timer() - t)
        return response.items

    def patch_namespaced_deployment(self, name, namespace, body):
        """Wrapper for `kubernetes.client.patch_namespaced_deployment`"""
        t = timeit.default_timer()
        try:
            kube_client = self.get_apps_v1_client()
            response = kube_client.patch_namespaced_deployment(
                name, namespace, body)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `patch_namespaced_deployment`: '
                              '%s', type(err).__name__, err)
            raise err
        self.logger.debug('Patched deployment `%s` in namespace `%s` with body'
                          ' `%s` in %s seconds.', name, namespace, body,
                          timeit.default_timer() - t)
        return response

    def patch_namespaced_job(self, name, namespace, body):
        """Wrapper for `kubernetes.client.patch_namespaced_job`"""
        t = timeit.default_timer()
        try:
            kube_client = self.get_batch_v1_client()
            response = kube_client.patch_namespaced_job(
                name, namespace, body)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('%s when calling `patch_namespaced_job`: %s',
                              type(err).__name__, err)
            raise err
        self.logger.debug('Patched job `%s` in namespace `%s` with body `%s` '
                          'in %s seconds.', name, namespace, body,
                          timeit.default_timer() - t)
        return response

    def get_current_pods(self, namespace, resource_type, name,
                         only_running=False):
        """Find the number of current pods deployed for the given resource.

        Args:
            name: str, name of the resource.
            namespace: str, namespace of the resource.
            resource_type: str, type of resource to count.
            only_running: bool, Only count pods with status `Running`.

        Returns:
            int, number of pods for the given resource.
        """
        if resource_type not in self.managed_resource_types:
            raise ValueError(
                '`resource_type` must be one of {}. Got {}.'.format(
                    self.managed_resource_types, resource_type))

        current_pods = 0
        if resource_type == 'deployment':
            deployments = self.list_namespaced_deployment(namespace)
            for d in deployments:
                if d.metadata.name == name:
                    if only_running:
                        current_pods = d.status.available_replicas
                    else:
                        current_pods = d.spec.replicas

                    self.logger.debug('Deployment %s has %s pods',
                                      name, current_pods)
                    break

        elif resource_type == 'job':
            jobs = self.list_namespaced_job(namespace)
            for j in jobs:
                if j.metadata.name == name:
                    current_pods = j.spec.parallelism  # TODO: is this right?
                    break

        if current_pods is None:  # status.available_replicas may be None
            current_pods = 0

        return int(current_pods)

    def clip_pod_count(self, desired_pods, min_pods, max_pods, current_pods):
        # set `desired_pods` to inside the max/min boundaries.
        _original = desired_pods
        if desired_pods > max_pods:
            desired_pods = max_pods
        elif desired_pods < min_pods:
            desired_pods = min_pods

        # To avoid removing currently running pods, wait until all
        # pods of the deployment are idle before scaling down.
        if 0 < desired_pods < current_pods:
            desired_pods = current_pods

        if desired_pods != _original:
            self.logger.debug('Clipped pods from %s to %s',
                              _original, desired_pods)
        return desired_pods

    def get_desired_pods(self, key, keys_per_pod,
                         min_pods, max_pods, current_pods):
        desired_pods = self.redis_keys[key] // keys_per_pod
        return self.clip_pod_count(desired_pods, min_pods,
                                   max_pods, current_pods)

    def scale_resource(self, desired_pods, current_pods,
                       resource_type, namespace, name):

        if resource_type not in self.managed_resource_types:
            raise ValueError('Cannot scale resource type: %s' % resource_type)

        if desired_pods == current_pods:
            return  # no scaling action is required

        if resource_type == 'job':
            # TODO: Find a suitable method for scaling jobs
            body = {'spec': {'parallelism': desired_pods}}
            res = self.patch_namespaced_job(name, namespace, body)

        if resource_type == 'deployment':
            body = {'spec': {'replicas': desired_pods}}
            res = self.patch_namespaced_deployment(name, namespace, body)

        self.logger.info('Successfully scaled %s `%s` in namespace `%s` '
                         'from %s to %s pods.', resource_type, name,
                         namespace, current_pods, desired_pods)
        return True

    def scale(self, namespace, resource_type, name,
              min_pods=0, max_pods=1, keys_per_pod=1):

        self.tally_queues()

        self.logger.debug('Scaling %s `%s.%s`.',
                          resource_type, namespace, name)

        current_pods = self.get_current_pods(namespace, resource_type, name)

        desired_pods = 0
        for key in self.redis_keys:
            desired_pods += self.get_desired_pods(key, keys_per_pod, min_pods,
                                                  max_pods, current_pods)

        desired_pods = self.clip_pod_count(desired_pods, min_pods,
                                           max_pods, current_pods)

        self.logger.debug('%s `%s` in namespace `%s` has a current state '
                          'of %s pods and a desired state of %s pods.',
                          str(resource_type).capitalize(), name,
                          namespace, current_pods, desired_pods)

        try:
            self.scale_resource(desired_pods, current_pods,
                                resource_type, namespace, name)
        except kubernetes.client.rest.ApiException as err:
            self.logger.warning('Failed to scale %s `%s.%s` due to %s: %s',
                                resource_type, namespace, name,
                                type(err).__name__, err)
