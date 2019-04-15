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
        scaling_config: string, joined lists of autoscaling configurations
        secondary_scaling_config: string, defines initial pod counts
        backoff_seconds: int, after a redis/subprocess error, sleep for this
            many seconds and retry the command.
        deployment_delim: string, character delimiting deployment configs.
        param_delim: string, character delimiting deployment config parameters.
    """

    def __init__(self, redis_client, scaling_config, secondary_scaling_config,
                 backoff_seconds=1, deployment_delim=';', param_delim='|'):

        if deployment_delim == param_delim:
            raise ValueError('`deployment_delim` and `param_delim` must be '
                             'different. Got "{}" and "{}".'.format(
                                 deployment_delim, param_delim))
        self.redis_client = redis_client
        self.backoff_seconds = int(backoff_seconds)
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.completed_statuses = {'done', 'failed'}

        self.autoscaling_params = self._get_autoscaling_params(
            scaling_config=scaling_config.rstrip(),
            deployment_delim=deployment_delim,
            param_delim=param_delim)

        self.secondary_autoscaling_params = self._get_autoscaling_params(
            scaling_config=secondary_scaling_config.rstrip(),
            deployment_delim=deployment_delim,
            param_delim=param_delim)

        self.redis_keys = {
            'predict': 0,
            'train': 0
        }

        self.managed_resource_types = {'deployment', 'job'}

        self.previous_reference_pods = {}

    def _get_autoscaling_params(self, scaling_config,
                                deployment_delim=';',
                                param_delim='|'):
        return [x.split(param_delim)
                for x in scaling_config.split(deployment_delim)]

    def scan_iter(self, match=None, count=1000):
        while True:
            try:
                response = self.redis_client.scan_iter(match=match, count=count)
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

        self.logger.debug('Tallying keys in redis matching: `%s`',
                          ', '.join(self.redis_keys.keys()))

        for key in self.scan_iter(count=1000):
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
        t = timeit.default_timer()
        kubernetes.config.load_incluster_config()
        kube_client = kubernetes.client.AppsV1Api()
        self.logger.debug('Created AppsV1Api client in %s seconds.',
                          timeit.default_timer() - t)
        return kube_client

    def get_batch_v1_client(self):
        """Returns Kubernetes API Client for AppsV1Api"""
        t = timeit.default_timer()
        kubernetes.config.load_incluster_config()
        kube_client = kubernetes.client.BatchV1Api()
        self.logger.debug('Created BatchV1Api client in %s seconds.',
                          timeit.default_timer() - t)
        return kube_client

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
        self.logger.debug('Found %s deployments in namespace `%s` in '
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
        """Find the number of current pods deployed for the given resource"""
        if resource_type not in self.managed_resource_types:
            raise ValueError('The resource_type of {} is unsuitable. Use either'
                             '`deployment` or `job`'.format(resource_type))

        current_pods = 0
        if resource_type == 'deployment':
            deployments = self.list_namespaced_deployment(namespace)
            for d in deployments:
                if d.metadata.name == name:
                    if only_running:
                        current_pods = d.status.available_replicas
                    else:
                        current_pods = d.spec.replicas
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
        if desired_pods > max_pods:
            desired_pods = max_pods
        elif desired_pods < min_pods:
            desired_pods = min_pods

        # To avoid removing currently running pods, wait until all
        # pods of the deployment are idle before scaling down.
        if 0 < desired_pods < current_pods:
            desired_pods = current_pods

        return desired_pods

    def get_desired_pods(self, key, keys_per_pod,
                         min_pods, max_pods, current_pods):
        desired_pods = self.redis_keys[key] // keys_per_pod
        return self.clip_pod_count(desired_pods, min_pods,
                                   max_pods, current_pods)

    def get_secondary_desired_pods(self, reference_pods, pods_per_reference_pod,
                                   min_pods, max_pods, current_pods):
        desired_pods = current_pods + pods_per_reference_pod * reference_pods
        return self.clip_pod_count(desired_pods, min_pods,
                                   max_pods, current_pods)

    def scale_resource(self, desired_pods, current_pods,
                       resource_type, namespace, name):
        if desired_pods == current_pods:
            return  # no scaling action is required

        if resource_type == 'job':
            # TODO: Find a suitable method for scaling jobs
            body = {'spec': {'parallelism': desired_pods}}
            res = self.patch_namespaced_job(name, namespace, body)

        elif resource_type == 'deployment':
            body = {'spec': {'replicas': desired_pods}}
            res = self.patch_namespaced_deployment(name, namespace, body)

        self.logger.info('Successfully scaled %s `%s` in namespace `%s` '
                         'from %s to %s pods.', resource_type, name,
                         namespace, current_pods, desired_pods)
        return True

    def scale_primary_resources(self):
        """Scale each resource defined in `autoscaling_params`"""
        for entry in self.autoscaling_params:
            # entry schema: minPods maxPods keysPerPod namespace resource_type
            #               predict_or_train deployment
            try:
                min_pods = int(entry[0])
                max_pods = int(entry[1])
                keys_per_pod = int(entry[2])
                namespace = str(entry[3]).strip()
                resource_type = str(entry[4]).strip()
                predict_or_train = str(entry[5]).strip()
                name = str(entry[6]).strip()
            except (IndexError, ValueError):
                self.logger.error('Autoscaling entry %s is malformed.', entry)
                continue

            self.logger.debug('Scaling %s `%s`', resource_type, name)

            current_pods = self.get_current_pods(
                namespace, resource_type, name)

            desired_pods = self.get_desired_pods(
                predict_or_train, keys_per_pod,
                min_pods, max_pods, current_pods)

            self.logger.debug('%s `%s` in namespace `%s` has a current state '
                              'of %s pods and a desired state of %s pods.',
                              str(resource_type).capitalize(), name,
                              namespace, current_pods, desired_pods)

            self.scale_resource(desired_pods, current_pods,
                                resource_type, namespace, name)

    def scale_secondary_resources(self):
        for entry in self.secondary_autoscaling_params:
            # redis-consumer-deployment|deployment|deepcell|
            # tf-serving-deployment|deployment|deepcell|7|0|200
            try:
                resource_name = str(entry[0]).strip()
                resource_type = str(entry[1]).strip()
                resource_namespace = str(entry[2]).strip()
                reference_resource_name = str(entry[3]).strip()
                reference_resource_type = str(entry[4]).strip()
                reference_resource_namespace = str(entry[5]).strip()
                pods_per_other_pod = int(entry[6])
                min_pods = int(entry[7])
                max_pods = int(entry[8])
            except (IndexError, ValueError):
                self.logger.error('Secondary autoscaling entry %s is '
                                  'malformed.', entry)
                continue

            self.logger.debug('Scaling secondary %s `%s`',
                              resource_type, resource_name)

            # keep track of how many reference pods we're working with
            if resource_name not in self.previous_reference_pods:
                self.previous_reference_pods[resource_name] = 0

            current_reference_pods = self.get_current_pods(
                reference_resource_namespace,
                reference_resource_type,
                reference_resource_name,
                only_running=True)

            new_reference_pods = current_reference_pods - \
                self.previous_reference_pods[resource_name]

            self.logger.debug('Secondary scaling: %s `%s` references %s `%s` '
                              'which has %s pods (%s new pods).',
                              str(resource_type).capitalize(), resource_name,
                              reference_resource_type, reference_resource_name,
                              current_reference_pods, new_reference_pods)

            # only scale secondary deployments if there are new reference pods
            if new_reference_pods > 0:
                # update reference pod count
                self.previous_reference_pods[resource_name] = current_reference_pods

                # compute desired pods for this deployment
                current_pods = self.get_current_pods(
                    resource_namespace, resource_type, resource_name)

                desired_pods = self.get_secondary_desired_pods(
                    new_reference_pods, pods_per_other_pod,
                    min_pods, max_pods, current_pods)

                self.scale_resource(desired_pods, current_pods, resource_type,
                                    resource_namespace, resource_name)

    def scale(self):
        self.tally_keys()
        self.scale_primary_resources()
        self.scale_secondary_resources()
