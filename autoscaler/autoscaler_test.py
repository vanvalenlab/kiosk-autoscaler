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

import pytest
import kubernetes

import autoscaler


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyRedis(object):

    def __init__(self, prefix='predict', status='new'):
        self.prefix = '/'.join(x for x in prefix.split('/') if x)
        self.status = status
        self.fail_count = 0

    def keys(self):
        return [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'failed', 'x.zip'),
            '{}_{}_{}'.format('train', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'done', 'x.tiff'),
            '{}_{}_{}'.format('train', self.status, 'x.zip'),
        ]

    def scan_iter(self, match=None, count=None):
        keys = [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'failed', 'x.zip'),
            '{}_{}_{}'.format('train', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'done', 'x.tiff'),
            '{}_{}_{}'.format('train', self.status, 'x.zip'),
            'malformedKey'
        ]
        if match:
            return (k for k in keys if k.startswith(match[:-1]))
        return (k for k in keys)

    def expected_keys(self, suffix=None):
        for k in self.keys():
            v = k.split('_')
            if v[0] == self.prefix:
                if v[1] == self.status:
                    if suffix:
                        if v[-1].lower().endswith(suffix):
                            yield k
                    else:
                        yield k

    def hget(self, rhash, field):
        if field == 'status':
            return rhash.split('_')[1]
        elif field == 'file_name':
            return rhash.split('_')[-1]
        elif field == 'input_file_name':
            return rhash.split('_')[-1]
        elif field == 'output_file_name':
            return rhash.split('_')[-1]
        return False

    def type(self, rhash):
        return 'hash'


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

    def test__get_primary_autoscaling_params(self):
        primary_params = """
            1|2|3|namespace|resource_1|predict|name_1;
            4|5|6|namespace|resource_1|track|name_1;
            7|8|9|namespace|resource_2|train|name_1
            """.strip()
        secondary_params = """
            redis-consumer-deployment|deployment|deepcell|tf-serving-deployment|deployment|deepcell|7|0|0;
            tracking-consumer-deployment|deployment|deepcell|tf-serving-deployment|deployment|deepcell|7|0|0;
            data-processing-deployment|deployment|deepcell|tf-serving-deployment|deployment|deepcell|1|0|0
            """.strip()

        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client,
                                       primary_params,
                                       secondary_params)
        print("printing primary the nsecondary")
        print(scaler.autoscaling_params)
        print(scaler.secondary_autoscaling_params)
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

    def test_get_secondary_desired_pods(self):
        # reference_pods, pods_per_reference_pod,
        # min_pods, max_pods, current_pods
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')

        # desired_pods is > max_pods
        desired_pods = scaler.get_secondary_desired_pods(1, 10, 0, 4, 4)
        assert desired_pods == 4
        # desired_pods is < min_pods
        desired_pods = scaler.get_secondary_desired_pods(1, 1, 2, 4, 0)
        assert desired_pods == 2
        # desired_pods is in range
        desired_pods = scaler.get_secondary_desired_pods(1, 3, 0, 4, 0)
        assert desired_pods == 3
        # desired_pods is in range with current pods and max limit
        desired_pods = scaler.get_secondary_desired_pods(1, 3, 0, 4, 2)
        assert desired_pods == 4
        # desired_pods is in range with current pods and no max limit
        desired_pods = scaler.get_secondary_desired_pods(1, 3, 0, 10, 2)
        assert desired_pods == 5
        # desired_pods is less than current_pods but current > max
        desired_pods = scaler.get_secondary_desired_pods(1, 0, 0, 4, 10)
        assert desired_pods == 10

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

    def test_tally_keys(self):
        redis_client = DummyRedis()
        scaler = autoscaler.Autoscaler(redis_client, 'None', 'None')
        scaler.tally_keys()
        assert scaler.redis_keys == {'predict': 2, 'track': 0, 'train': 2}

    def test_scale_primary_resources(self):
        redis_client = DummyRedis()
        deploy_params = ['0', '1', '3', 'ns', 'deployment', 'predict', 'name']
        job_params = ['1', '2', '1', 'ns', 'job', 'train', 'name']

        params = [deploy_params, job_params]

        param_delim = '|'
        deployment_delim = ';'

        # non-integer values will warn, but not raise (or autoscale)
        bad_params = ['f0', 'f1', 'f3', 'ns', 'job', 'train', 'name']
        p = deployment_delim.join([param_delim.join(bad_params)])
        scaler = autoscaler.Autoscaler(redis_client, p, 'None',
                                       deployment_delim, param_delim)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes
        scaler.scale_primary_resources()

        # not enough params will warn, but not raise (or autoscale)
        bad_params = ['0', '1', '3', 'ns', 'job', 'train']
        p = deployment_delim.join([param_delim.join(bad_params)])
        scaler = autoscaler.Autoscaler(redis_client, p, 'None',
                                       deployment_delim, param_delim)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes
        scaler.scale_primary_resources()

        # test bad resource_type
        with pytest.raises(ValueError):
            bad_params = ['0', '1', '3', 'ns', 'bad_type', 'train', 'name']
            p = deployment_delim.join([param_delim.join(bad_params)])
            scaler = autoscaler.Autoscaler(redis_client, p, 'None',
                                           deployment_delim, param_delim)
            scaler.get_apps_v1_client = DummyKubernetes
            scaler.get_batch_v1_client = DummyKubernetes
            scaler.scale_primary_resources()

        # test good delimiters and scaling params, bad resource_type
        deploy_params = ['0', '5', '1', 'ns', 'deployment', 'predict', 'name']
        job_params = ['1', '2', '1', 'ns', 'job', 'train', 'name']
        params = [deploy_params, job_params]
        p = deployment_delim.join([param_delim.join(p) for p in params])

        scaler = autoscaler.Autoscaler(redis_client, p, 'None',
                                       deployment_delim, param_delim)

        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes

        scaler.scale_primary_resources()
        # test desired_pods == current_pods
        scaler.get_desired_pods = lambda *x: 4
        scaler.scale_primary_resources()

        # same delimiter throws an error;
        with pytest.raises(ValueError):
            param_delim = '|'
            deployment_delim = '|'
            p = deployment_delim.join([param_delim.join(p) for p in params])
            autoscaler.Autoscaler(None, p, 'None',
                                  deployment_delim, param_delim)

    def test_scale_secondary_resources(self):
        # redis-deployment|deployment|namespace|tf-serving-deployment|
        # deployment|namespace2|podRatio|minPods|maxPods

        redis_client = DummyRedis()
        deploy_params = ['name', 'deployment', 'ns',
                         'primary', 'deployment', 'ns',
                         '7', '1', '10']
        job_params = ['name', 'job', 'ns',
                      'primary', 'job', 'ns',
                      '7', '1', '10']

        params = [deploy_params, job_params]

        param_delim = '|'
        deployment_delim = ';'

        # non-integer values will warn, but not raise (or autoscale)
        bad_params = ['name', 'bad_type', 'ns',
                      'primary', 'bad_type', 'ns',
                      'f7', 'f1', 'f10']
        p = deployment_delim.join([param_delim.join(bad_params)])
        scaler = autoscaler.Autoscaler(redis_client, 'None', p,
                                       deployment_delim, param_delim)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes
        scaler.scale_secondary_resources()

        # not enough params will warn, but not raise (or autoscale)
        bad_params = ['name', 'job', 'ns', 'primary', 'job', '7', '1', '10']
        p = deployment_delim.join([param_delim.join(bad_params)])
        scaler = autoscaler.Autoscaler(redis_client, 'None', p,
                                       deployment_delim, param_delim)
        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes
        scaler.scale_secondary_resources()

        # test bad resource_type
        with pytest.raises(ValueError):
            bad_params = ['name', 'bad_type', 'ns',
                          'primary', 'bad_type', 'ns',
                          '7', '1', '10']
            p = deployment_delim.join([param_delim.join(bad_params)])
            scaler = autoscaler.Autoscaler(redis_client, 'None', p,
                                           deployment_delim, param_delim)
            scaler.get_apps_v1_client = DummyKubernetes
            scaler.get_batch_v1_client = DummyKubernetes
            scaler.scale_secondary_resources()

        # test good delimiters and scaling params, bad resource_type
        params = [deploy_params, job_params]
        p = deployment_delim.join([param_delim.join(p) for p in params])

        scaler = autoscaler.Autoscaler(redis_client, 'None', p,
                                       deployment_delim, param_delim)

        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes

        scaler.scale_secondary_resources()
        # test desired_pods == current_pods
        scaler.get_desired_pods = lambda *x: 4
        scaler.scale_secondary_resources()

        # test nothing happens if no new pods
        params = [deploy_params, job_params]
        p = deployment_delim.join([param_delim.join(p) for p in params])

        scaler = autoscaler.Autoscaler(redis_client, 'None', p,
                                       deployment_delim, param_delim)

        scaler.get_apps_v1_client = DummyKubernetes
        scaler.get_batch_v1_client = DummyKubernetes

        def curr_pods(*args, **kwargs):
            if args[2] == 'primary':
                return 0
            return 2

        scaler.get_current_pods = curr_pods

        global counter
        counter = 0

        def dummy_scale(*args, **kwargs):
            global counter
            counter += 1

        scaler.scale_resource = dummy_scale
        scaler.scale_secondary_resources()
        # `scale_deployments` should not be called.
        assert counter == 0

        # test scaling does occur with current reference pods
        def curr_pods_2(*args, **kwargs):
            if args[2] == 'primary':
                return 1
            return 2
        scaler.get_current_pods = curr_pods_2
        scaler.scale_secondary_resources()
        # `scale_deployments` should be called.
        assert counter == 1

        # same delimiter throws an error;
        with pytest.raises(ValueError):
            param_delim = '|'
            deployment_delim = '|'
            p = deployment_delim.join([param_delim.join(p) for p in params])
            autoscaler.Autoscaler(None, 'None', p,
                                  deployment_delim, param_delim)

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

        scaler.tally_keys = dummy_tally
        scaler.scale_primary_resources = dummy_scale_resources
        scaler.scale_secondary_resources = dummy_scale_resources

        scaler.scale()
        assert counter == 3
