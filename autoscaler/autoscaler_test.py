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
"""Tests for the Autoscaler class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import string

import pytest
import kubernetes

import autoscaler


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyRedis(object):

    def llen(self, queue_name):
        return len(queue_name)

    def scan_iter(self, match=None, count=None):
        match = match if match else ''
        yield match + random.choice(string.ascii_letters)


class DummyKubernetes(object):

    def __init__(self, fail=False):
        self.fail = fail

    def list_namespaced_deployment(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return Bunch(items=[
            Bunch(spec=Bunch(replicas='4'),
                  metadata=Bunch(name='pod1'),
                  status=Bunch(available_replicas=None)),
            Bunch(spec=Bunch(replicas='8'),
                  metadata=Bunch(name='pod2'),
                  status=Bunch(available_replicas='8')),
        ])

    def list_namespaced_job(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return Bunch(items=[
            Bunch(spec=Bunch(completions='1', parallelism='1'),
                  metadata=Bunch(name='pod1')),
            Bunch(spec=Bunch(completions='2', parallelism='2'),
                  metadata=Bunch(name='pod2'))
        ])

    def patch_namespaced_deployment(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return Bunch(items=[Bunch(spec=Bunch(replicas='4'),
                                  metadata=Bunch(name='pod'))])

    def patch_namespaced_job(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return Bunch(items=[Bunch(spec=Bunch(completions='0', parallelism='0'),
                                  metadata=Bunch(name='pod'))])


class TestAutoscaler(object):

    def test_get_desired_pods(self):
        # key, keys_per_pod, min_pods, max_pods, current_pods
        redis_client = DummyRedis()
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

    def test_list_namespaced_deployment(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test ApiException is logged and thrown
        scaler.get_apps_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.list_namespaced_deployment('ns')

    def test_list_namespaced_job(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test ApiException is logged and thrown
        scaler.get_batch_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.list_namespaced_job('ns')

    def test_patch_namespaced_deployment(self):
        redis_client = DummyRedis()
        spec = {'spec': {'replicas': 1}}
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test successful patch
        scaler.get_apps_v1_client = lambda: DummyKubernetes(fail=False)
        scaler.patch_namespaced_deployment('job', 'ns', spec)
        # test ApiException is logged and thrown
        scaler.get_apps_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.patch_namespaced_deployment('pod', 'ns', spec)

    def test_patch_namespaced_job(self):
        redis_client = DummyRedis()
        spec = {'spec': {'parallelism': 1}}
        scaler = autoscaler.Autoscaler(redis_client, 'queue')
        # test successful patch
        scaler.get_batch_v1_client = lambda: DummyKubernetes(fail=False)
        scaler.patch_namespaced_job('job', 'ns', spec)
        # test ApiException is logged and thrown
        scaler.get_batch_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.patch_namespaced_job('job', 'ns', spec)

    def test_get_current_pods(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'queue')

        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes

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

    def test_tally_queues(self):
        redis_client = DummyRedis()
        qd = ','
        expected = lambda x: sum([len(q) for q in x.split(qd)])

        queue = 'queue'
        scaler = autoscaler.Autoscaler(
            redis_client, queues=queue, queue_delim=qd)

        scaler.tally_queues()
        assert scaler.redis_keys == {queue: expected(queue) + 1}

        redis_client = DummyRedis()
        queue = 'predict,track,train'
        scaler = autoscaler.Autoscaler(
            redis_client, queues=queue, queue_delim=qd)
        scaler.tally_queues()

        expected_keys = {k: expected(k) + 1 for k in queue.split(qd)}
        assert scaler.redis_keys == expected_keys

    def test_scale_resource(self):
        # pylint: disable=E1111
        redis_client = DummyRedis()
        redis_client = DummyRedis()

        namespace = 'dummy-namespace'
        name = 'dummy-name'
        scaler = autoscaler.Autoscaler(redis_client, 'queue')

        # moneky-patch kubernetes API commands.
        scaler.patch_namespaced_deployment = lambda *x: True
        scaler.patch_namespaced_job = lambda *x: True

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

    def test_scale(self):

        scale_kwargs = {
            'namespace': 'namespace',
            'name': 'test',
        }

        queues = 'predict,track'
        qd = ','

        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(
            redis_client, queues=queues, queue_delim=qd)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes

        # test successful scale
        for resource_type in scaler.managed_resource_types:
            scaler.scale(resource_type=resource_type, **scale_kwargs)

        # test failed scale
        def bad_scale_resource(*args, **kwargs):
            raise kubernetes.client.rest.ApiException('thrown on purpose')

        scaler.scale_resource = bad_scale_resource
        for resource_type in scaler.managed_resource_types:
            scaler.scale(resource_type=resource_type, **scale_kwargs)
