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
"""Tests for the Autoscaler class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random

import pytest
import kubernetes
import fakeredis

import autoscaler


@pytest.fixture
def redis_client():
    yield fakeredis.FakeStrictRedis()


def kube_error(*_, **__):
    raise kubernetes.client.rest.ApiException('thrown on purpose')


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyKubernetes(object):
    # pylint: disable=R0201

    def list_namespaced_deployment(self, *_, **__):
        return Bunch(items=[
            Bunch(spec=Bunch(replicas='4'),
                  metadata=Bunch(name='pod1'),
                  status=Bunch(available_replicas=None)),
            Bunch(spec=Bunch(replicas='8'),
                  metadata=Bunch(name='pod2'),
                  status=Bunch(available_replicas='8')),
        ])

    def list_namespaced_job(self, *_, **__):
        return Bunch(items=[
            Bunch(spec=Bunch(completions='1', parallelism='1'),
                  metadata=Bunch(name='pod1')),
            Bunch(spec=Bunch(completions='2', parallelism='2'),
                  metadata=Bunch(name='pod2'))
        ])

    def patch_namespaced_deployment(self, *_, **__):
        return Bunch(items=[Bunch(spec=Bunch(replicas='4'),
                                  metadata=Bunch(name='pod'))])

    def patch_namespaced_job(self, *_, **__):
        return Bunch(items=[Bunch(spec=Bunch(completions='0', parallelism='0'),
                                  metadata=Bunch(name='pod'))])


class TestAutoscaler(object):
    # pylint: disable=W0621,R0201

    def test_get_desired_pods(self, redis_client):
        # key, keys_per_pod, min_pods, max_pods, current_pods
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        scaler.redis_keys['queue'] = 10
        # desired_pods is > max_pods
        desired_pods = scaler.get_desired_pods('queue', 2, 0, 2, 1)
        assert desired_pods == 2
        # desired_pods is < min_pods
        desired_pods = scaler.get_desired_pods('queue', 5, 9, 10, 0)
        assert desired_pods == 9
        # desired_pods is in range
        desired_pods = scaler.get_desired_pods('queue', 3, 0, 5, 1)
        assert desired_pods == 3
        # desired_pods is in range, current_pods exist
        desired_pods = scaler.get_desired_pods('queue', 10, 0, 5, 3)
        assert desired_pods == 3

    def test_list_namespaced_deployment(self, mocker, redis_client):
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.AppsV1Api', DummyKubernetes)

        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test ApiException is logged and thrown
        mocker.patch('kubernetes.client.AppsV1Api.list_namespaced_deployment',
                     kube_error)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.list_namespaced_deployment('ns')

    def test_list_namespaced_job(self, mocker, redis_client):
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.BatchV1Api', DummyKubernetes)

        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test ApiException is logged and thrown
        mocker.patch('kubernetes.client.BatchV1Api.list_namespaced_job',
                     kube_error)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.list_namespaced_job('ns')

    def test_patch_namespaced_deployment(self, mocker, redis_client):
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.AppsV1Api', DummyKubernetes)

        spec = {'spec': {'replicas': 1}}
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test successful patch
        scaler.patch_namespaced_deployment('job', 'ns', spec)

        # test ApiException is logged and thrown
        mocker.patch('kubernetes.client.AppsV1Api.patch_namespaced_deployment',
                     kube_error)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.patch_namespaced_deployment('pod', 'ns', spec)

    def test_patch_namespaced_job(self, mocker, redis_client):
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.BatchV1Api', DummyKubernetes)

        spec = {'spec': {'parallelism': 1}}
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test successful patch
        scaler.patch_namespaced_job('job', 'ns', spec)
        # test ApiException is logged and thrown
        mocker.patch('kubernetes.client.BatchV1Api.patch_namespaced_job',
                     kube_error)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.patch_namespaced_job('job', 'ns', spec)

    def test_get_current_pods(self, mocker, redis_client):
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.AppsV1Api', DummyKubernetes)
        mocker.patch('kubernetes.client.BatchV1Api', DummyKubernetes)

        scaler = autoscaler.Autoscaler(redis_client, 'queue')

        # test invalid resource_type
        with pytest.raises(ValueError):
            scaler.get_current_pods('namespace', 'bad_type', 'pod')

        deployed_pods = scaler.get_current_pods('ns', 'deployment', 'pod1')
        assert deployed_pods == 4

        deployed_pods = scaler.get_current_pods('ns', 'deployment', 'pod2')
        assert deployed_pods == 8

        deployed_pods = scaler.get_current_pods('ns', 'deployment', 'pod2', True)
        assert deployed_pods == 8

        deployed_pods = scaler.get_current_pods('ns', 'deployment', 'pod1', True)
        assert deployed_pods == 0

        deployed_pods = scaler.get_current_pods('ns', 'job', 'pod1')
        assert deployed_pods == 1

        deployed_pods = scaler.get_current_pods('ns', 'job', 'pod2')
        assert deployed_pods == 2

    def test_tally_queues(self, redis_client):
        qd = ','

        queue = 'queue'
        expected = random.randint(1, 10)
        for q in queue.split(qd):
            for _ in range(expected):
                redis_client.lpush(q, 'jobHash')

        scaler = autoscaler.Autoscaler(
            redis_client, queues=queue, queue_delim=qd)

        scaler.tally_queues()
        assert scaler.redis_keys == {queue: expected}

        queue = 'predict,track,train'
        expected = random.randint(1, 10)
        for q in queue.split(qd):
            for _ in range(expected):
                redis_client.lpush(q, 'jobHash')

        scaler = autoscaler.Autoscaler(
            redis_client, queues=queue, queue_delim=qd)
        scaler.tally_queues()

        expected_keys = {q: expected for q in queue.split(qd)}
        assert scaler.redis_keys == expected_keys

    def test_scale_resource(self, mocker, redis_client):
        # pylint: disable=E1111
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.AppsV1Api', DummyKubernetes)
        mocker.patch('kubernetes.client.BatchV1Api', DummyKubernetes)

        namespace = 'dummy-namespace'
        name = 'dummy-name'
        scaler = autoscaler.Autoscaler(redis_client, 'queue')

        # if desired == current, no action is taken.
        res = scaler.scale_resource(1, 1, 'deployment', namespace, name)
        assert not res

        # scale a job
        res = scaler.scale_resource(2, 1, 'job', namespace, name)
        assert res

        # scale a deployment
        res = scaler.scale_resource(2, 1, 'deployment', namespace, name)
        assert res

        # bad resource type
        with pytest.raises(ValueError):
            scaler.scale_resource(2, 1, 'badvalue', namespace, name)

    def test_scale(self, mocker, redis_client):
        mocker.patch('kubernetes.config.load_incluster_config')
        mocker.patch('kubernetes.client.AppsV1Api', DummyKubernetes)
        mocker.patch('kubernetes.client.BatchV1Api', DummyKubernetes)

        scale_kwargs = {
            'namespace': 'namespace',
            'name': 'test',
        }

        queues = 'predict,track'
        qd = ','

        scaler = autoscaler.Autoscaler(
            redis_client, queues=queues, queue_delim=qd)

        # test successful scale
        for resource_type in scaler.managed_resource_types:
            scaler.scale(resource_type=resource_type, **scale_kwargs)

        # test failed scale
        def bad_scale_resource(*args, **kwargs):
            raise kubernetes.client.rest.ApiException('thrown on purpose')

        scaler.scale_resource = bad_scale_resource
        for resource_type in scaler.managed_resource_types:
            scaler.scale(resource_type=resource_type, **scale_kwargs)
