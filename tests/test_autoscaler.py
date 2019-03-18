class DummyRedis(object):
    def __init__(self, prefix='predict', status='new'):
        self.prefix = '/'.join(x for x in prefix.split('/') if x)
        self.status = status

    def keys(self):
        return [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.zip'),
            '{}_{}_{}'.format('other', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.tiff'),
            '{}_{}_{}'.format('other', self.status, 'x.zip'),
        ]

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

    def hmset(self, rhash, hvals):  # pylint: disable=W0613
        return hvals

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

    def hset(self, rhash, status, value):  # pylint: disable=W0613
        return {status: value}

    def hgetall(self, rhash):  # pylint: disable=W0613
        return {
                b'identity_outputting':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'identity_predicting':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'identity_started':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'identity_upload':
                b'bucket-monitor-deployment-6c85bd58f8-tzfjn',
                b'model_version':
                b'0',
                b'output_url':
                b'https://storage.googleapis.com/deepcell-output-benchmarking/output/6e33c4e2ac2d7a4bbb5b75d5c6b299a5.zip',
                b'outputting_to_output_time':
                b'0.668991943359375',
                b'postprocess_function':
                b'watershed',
                b'postprocess_to_outputting_time':
                b'8.20991015625',
                b'start_to_preprocessing_time':
                b'0.218984619140625',
                b'timestamp_post-processing':
                b'1552909111738.856',
                b'timestamp_predicting':
                b'1552909106910.563',
                b'preprocessing_to_predicting_time':
                b'0.0022021484375',
                b'predicting_to_postprocess_time':
                b'4.82829296875',
                b'timestamp_preprocessing':
                b'1552909106908.3608',
                b'identity_post-processing':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'timestamp_started':
                b'1552909106689.3762',
                b'upload_to_start_time':
                b'2940.002716064453',
                b'timestamp_outputting':
                b'1552909119948.766',
                b'status':
                b'done',
                b'identity_preprocessing':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'timestamp_last_status_update':
                b'1552909120617.758',
                b'timestamp_output':
                b'1552909120617.758',
                b'url':
                b'https://storage.googleapis.com/deepcell-output-benchmarking/uploads/directupload_watershednuclearnofgbg41f16_0_watershed_0_qwbenchmarking100000special_image_0.png',
                b'input_file_name':
                b'uploads/directupload_watershednuclearnofgbg41f16_0_watershed_0_qwbenchmarking100000special_image_0.png',
                b'cuts':
                b'0',
                b'output_file_name':
                b'output/6e33c4e2ac2d7a4bbb5b75d5c6b299a5.zip',
                b'identity_output':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'timestamp_upload':
                b'1552906166686.6602',
                b'model_name':
                b'watershednuclearnofgbg41f16'}

    def type(self, key):  # pylint: disable=W0613
        return 'hash'

class TestAutoscaler(object):

    # if there's only 1 key, then we need to see
    # 1 redis-consumer
    # 1 data-processing
    # 1 tf-serving
    # 0 training-job
    def one_redis_entry(self):
        dumb_redis = DummyRedis()
        keys = dumb_redis.keys()

        autoscaler = autoscale.Autoscaler()
        autoscaler.redis_client = dumb_redis
        autoscaler.redis_client.keys = lambda x: {
                b'start_to_preprocessing_time':
                b'0.218984619140625',
                b'identity_upload':
                b'bucket-monitor-deployment-6c85bd58f8-tzfjn',
                b'identity_predicting':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'identity_started':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'timestamp_predicting':
                b'1552909106910.563',
                b'postprocess_function':
                b'watershed',
                b'model_version':
                b'0',
                b'preprocessing_to_predicting_time':
                b'0.0022021484375',
                b'timestamp_preprocessing':
                b'1552909106908.3608',
                b'timestamp_started':
                b'1552909106689.3762',
                b'upload_to_start_time':
                b'2940.002716064453',
                b'status':
                b'done',
                b'identity_preprocessing':
                b'redis-consumer-deployment-868d8596bb-mz57w',
                b'timestamp_last_status_update':
                b'1552909120617.758',
                b'url':
                b'https://storage.googleapis.com/deepcell-output-benchmarking/uploads/directupload_watershednuclearnofgbg41f16_0_watershed_0_qwbenchmarking100000special_image_0.png',
                b'input_file_name':
                b'uploads/directupload_watershednuclearnofgbg41f16_0_watershed_0_qwbenchmarking100000special_image_0.png',
                b'cuts':
                b'0',
                b'timestamp_upload':
                b'1552906166686.6602',
                b'model_name':
                b'watershednuclearnofgbg41f16'}
        assert True
