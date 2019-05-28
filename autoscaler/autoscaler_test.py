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

    def test__get_autoscaling_params(self):
        primary_params = """
            1|2|3|namespace|resource_1|predict|name_1;
            4|5|6|namespace|resource_1|track|name_1;
            7|8|9|namespace|resource_2|train|name_1
            """.strip()

        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client,
                                       primary_params)
        print("printing primary autoscaling parameters")
        print(scaler.autoscaling_params)
        print("done printing")
        assert scaler.autoscaling_params == {
            ('namespace', 'resource_1', 'name_1'): [
                {
                    "prefix": "predict",
                    "min_pods": 1,
                    "max_pods": 2,
                    "keys_per_pod": 3
                },
                {
                    "prefix": "track",
                    "min_pods": 4,
                    "max_pods": 5,
                    "keys_per_pod": 6
                },
            ],
            ('namespace', 'resource_2', 'name_1'): [
                {
                    "prefix": "train",
                    "min_pods": 7,
                    "max_pods": 8,
                    "keys_per_pod": 9
                }
            ]
        }

    def test_get_desired_pods(self):
        # key, keys_per_pod, min_pods, max_pods, current_pods
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        scaler.redis_keys['predict'] = 10
        # desired_pods is > max_pods
        desired_pods = scaler.get_desired_pods('predict', 2, 0, 2, 1)
        assert desired_pods == 2
        # desired_pods is < min_pods
        desired_pods = scaler.get_desired_pods('predict', 5, 9, 10, 0)
        assert desired_pods == 9
        # desired_pods is in range
        desired_pods = scaler.get_desired_pods('predict', 3, 0, 5, 1)
        assert desired_pods == 3
        # desired_pods is in range, current_pods exist
        desired_pods = scaler.get_desired_pods('predict', 10, 0, 5, 3)
        assert desired_pods == 3

    def test_list_namespaced_deployment(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        # test ApiException is logged and thrown
        scaler.get_apps_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.list_namespaced_deployment('ns')

    def test_list_namespaced_job(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        # test ApiException is logged and thrown
        scaler.get_batch_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.list_namespaced_job('ns')

    def test_patch_namespaced_deployment(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        # test ApiException is logged and thrown
        scaler.get_apps_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.patch_namespaced_deployment(
                'pod', 'ns', {'spec': {'replicas': 1}})

    def test_patch_namespaced_job(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        # test ApiException is logged and thrown
        scaler.get_batch_v1_client = lambda: DummyKubernetes(fail=True)
        with pytest.raises(kubernetes.client.rest.ApiException):
            scaler.patch_namespaced_job(
                'job', 'ns', {'spec': {'parallelism': 1}})

    def test_get_current_pods(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')

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
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        scaler.tally_queues()
        assert scaler.redis_keys == {'predict': 8, 'track': 6, 'train': 6}

    def test_scale_resources(self):
        redis_client = DummyRedis()
        deploy_params = ['0', '1', '3', 'ns', 'deployment', 'predict', 'name']
        job_params = ['1', '2', '1', 'ns', 'job', 'train', 'name']

        params = [deploy_params, job_params]

        param_delim = '|'
        deployment_delim = ';'

        # non-integer values will warn, but not raise (or autoscale)
        bad_params = ['f0', 'f1', 'f3', 'ns', 'job', 'train', 'name']
        p = deployment_delim.join([param_delim.join(bad_params)])
        scaler = autoscaler.Autoscaler(redis_client, p, deployment_delim,
                                       param_delim)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes
        scaler.scale_resources()

        # not enough params will warn, but not raise (or autoscale)
        bad_params = ['0', '1', '3', 'ns', 'job', 'train']
        p = deployment_delim.join([param_delim.join(bad_params)])
        scaler = autoscaler.Autoscaler(redis_client, p, deployment_delim,
                                       param_delim)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes
        scaler.scale_resources()

        # test bad resource_type
        with pytest.raises(ValueError):
            bad_params = ['0', '1', '3', 'ns', 'bad_type', 'train', 'name']
            p = deployment_delim.join([param_delim.join(bad_params)])
            scaler = autoscaler.Autoscaler(redis_client, p, deployment_delim,
                                           param_delim)
            scaler.get_apps_v1_client = DummyKubernetes
            scaler.get_batch_v1_client = DummyKubernetes
            scaler.scale_resources()

        # test good delimiters and scaling params, bad resource_type
        deploy_params = ['0', '5', '1', 'ns', 'deployment', 'predict', 'name']
        job_params = ['1', '2', '1', 'ns', 'job', 'train', 'name']
        params = [deploy_params, job_params]
        p = deployment_delim.join([param_delim.join(p) for p in params])

        scaler = autoscaler.Autoscaler(redis_client, p, deployment_delim,
                                       param_delim)

        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes

        scaler.scale_resources()
        # test desired_pods == current_pods
        scaler.get_desired_pods = lambda *x: 4
        scaler.scale_resources()

        # same delimiter throws an error;
        with pytest.raises(ValueError):
            param_delim = '|'
            deployment_delim = '|'
            p = deployment_delim.join([param_delim.join(p) for p in params])
            autoscaler.Autoscaler(None, p, deployment_delim, param_delim)

    def test_scale(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')

        global counter
        counter = 0

        def dummy_tally():
            global counter
            counter += 1

        def dummy_scale_resources():
            global counter
            counter += 1

        scaler.tally_queues = dummy_tally
        scaler.scale_resources = dummy_scale_resources

        scaler.scale()
        assert counter == 2
