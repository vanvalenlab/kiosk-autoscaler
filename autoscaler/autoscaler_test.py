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

import numpy as np
import redis
import pytest

import autoscaler


class DummyRedis(object):  # pylint: disable=useless-object-inheritance

    def __init__(self, prefix='predict', status='new', fail_tolerance=0):
        self.prefix = '/'.join(x for x in prefix.split('/') if x)
        self.status = status
        self.fail_count = 0
        self.fail_tolerance = fail_tolerance

    def keys(self):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'failed', 'x.zip'),
            '{}_{}_{}'.format('train', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'done', 'x.tiff'),
            '{}_{}_{}'.format('train', self.status, 'x.zip'),
        ]

    def scan_iter(self, match=None):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')

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
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        if field == 'status':
            return rhash.split('_')[1]
        elif field == 'file_name':
            return rhash.split('_')[-1]
        elif field == 'input_file_name':
            return rhash.split('_')[-1]
        elif field == 'output_file_name':
            return rhash.split('_')[-1]
        return False


class TestAutoscaler(object):  # pylint: disable=useless-object-inheritance

    def test_hget(self):
        redis_client = DummyRedis(fail_tolerance=2)
        scaler = autoscaler.Autoscaler(redis_client, 'None',
                                       backoff_seconds=0.01)
        data = scaler.hget('rhash_new', 'status')
        assert data == 'new'
        assert scaler.redis_client.fail_count == redis_client.fail_tolerance

    def test_scan_iter(self):
        prefix = 'predict'
        redis_client = DummyRedis(fail_tolerance=2, prefix=prefix)
        scaler = autoscaler.Autoscaler(redis_client, 'None',
                                       backoff_seconds=0.01)
        data = scaler.scan_iter(match=prefix)
        keys = [k for k in data]
        expected = [k for k in redis_client.keys() if k.startswith(prefix)]
        assert scaler.redis_client.fail_count == redis_client.fail_tolerance
        np.testing.assert_array_equal(keys, expected)

    def test_get_desired_pods(self):
        # key, keys_per_pod, min_pods, max_pods, current_pods
        redis_client = DummyRedis(fail_tolerance=2)
        scaler = autoscaler.Autoscaler(redis_client, 'None',
                                       backoff_seconds=0.01)
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

    def test_get_current_pods(self):
        redis_client = DummyRedis(fail_tolerance=2)
        scaler = autoscaler.Autoscaler(redis_client, 'None',
                                       backoff_seconds=0.01)

        # test invalid resource_type
        with pytest.raises(ValueError):
            scaler.get_current_pods('namespace', 'bad_type', 'deployment')

        deploy_example = 'other\ntext\nReplicas:  4 desired | 2 updated | ' + \
                         '1 total | 3 available | 0 unavailable\nmore\ntext\n'
        scaler._get_kubectl_output = lambda x: deploy_example
        deployed_pods = scaler.get_current_pods('ns', 'deployment', 'dep')
        assert deployed_pods == 4

        job_example = 'other\ntext\nCompletions:    33\nmore\ntext\n'
        scaler._get_kubectl_output = lambda x: job_example
        deployed_pods = scaler.get_current_pods('ns', 'job', 'dep')
        assert deployed_pods == 33

        job_example = 'other\ntext\nCompletions:  <unset>\nmore\ntext\n'
        scaler._get_kubectl_output = lambda x: job_example
        deployed_pods = scaler.get_current_pods('ns', 'job', 'dep')
        assert deployed_pods == 0

    def test_tally_keys(self):
        redis_client = DummyRedis(fail_tolerance=2)
        scaler = autoscaler.Autoscaler(redis_client, 'None',
                                       backoff_seconds=0.01)
        scaler.tally_keys()
        assert scaler.redis_keys == {'predict': 2, 'train': 2}

    def test__scale_deployments(self):
        redis_client = DummyRedis(fail_tolerance=2)
        deploy_params = ['0', '1', '3', 'ns', 'deployment', 'predict', 'name']
        job_params = ['1', '2', '1', 'ns', 'job', 'train', 'name']

        params = [deploy_params, job_params]

        # same delimiter throws an error;
        with pytest.raises(ValueError):
            param_delim = '|'
            deployment_delim = '|'
            p = deployment_delim.join([param_delim.join(p) for p in params])
            autoscaler.Autoscaler(None, p, 0, deployment_delim, param_delim)

        # non-integer values will warn, but will not raise (or autoscale)
        with pytest.raises(ValueError):
            bad_params = ['f0', 'f1', 'f3', 'ns', 'job', 'train', 'name']
            param_delim = '|'
            deployment_delim = ';'
            p = deployment_delim.join([param_delim.join(bad_params)])
            scaler = autoscaler.Autoscaler(redis_client, p, 0,
                                           deployment_delim,
                                           param_delim)
            scaler._scale_deployments()

        # bad resource_type
        with pytest.raises(ValueError):
            bad_params = ['0', '1', '3', 'ns', 'bad_type', 'train', 'name']
            param_delim = '|'
            deployment_delim = ';'
            p = deployment_delim.join([param_delim.join(bad_params)])
            scaler = autoscaler.Autoscaler(redis_client, p, 0,
                                           deployment_delim,
                                           param_delim)
            scaler._scale_deployments()

        # test good delimiters and scaling params, bad resource_type
        param_delim = '|'
        deployment_delim = ';'
        deploy_params = ['0', '1', '3', 'ns', 'deployment', 'predict', 'name']
        job_params = ['1', '2', '1', 'ns', 'job', 'train', 'name']
        params = [deploy_params, job_params]

        p = deployment_delim.join([param_delim.join(p) for p in deploy_params])
        print(p)
        do_nothing = lambda x: 1

        deploy_example = 'other\ntext\nReplicas:  4 desired | 2 updated | ' + \
                         '1 total | 3 available | 0 unavailable\nmore\ntext\n'
        scaler._get_kubectl_output = lambda x: deploy_example

        scaler.get_current_pods = do_nothing
        scaler._make_kubectl_call = do_nothing
        scaler.get_desired_pods = lambda x: 5
        scaler = autoscaler.Autoscaler(redis_client, p, 0,
                                       deployment_delim,
                                       param_delim)
        # scaler._scale_deployments()
