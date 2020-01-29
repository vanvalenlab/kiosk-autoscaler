# kiosk-autoscaler

[![Build Status](https://travis-ci.com/vanvalenlab/kiosk-autoscaler.svg?branch=master)](https://travis-ci.com/vanvalenlab/kiosk-autoscaler)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-autoscaler/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-autoscaler?branch=master)

The `kiosk-autoscaler` turns on GPU resources when they are required and turns them back off when they are not needed, minimizing GPU costs for the DeepCell Kiosk.

## Configuration

The Autoscaler is configured using environment variables. Please find a table of all environment variables and their descriptions below.

| Name | Description | Default Value |
| :--- | :--- | :--- |
| `RESOURCE_NAME` | **REQUIRED**: The name of the resource to scale. |  |
| `RESOURCE_TYPE` | The resource type of `RESOURCE_NAME`, one of `deployment` or `job`. | `deployment` |
| `RESOURCE_NAMESPACE` | The Kubernetes namespace of `RESOURCE_NAME`. | `default` |
| `QUEUES` | A `QUEUE_DELIMITER` separated list of work queues to monitor. | `predict,track` |
| `QUEUE_DELIMITER` | A string used to separate a list of queue names in `QUEUES`. | `,` |
| `INTERVAL` | How frequently the autoscaler checks for required resources, in seconds. | `5` |
| `REDIS_HOST` | The IP address or hostname of Redis. | `redis-master` |
| `REDIS_PORT` | The port used to connect to Redis. | `6379` |
| `REDIS_INTERVAL` | Time to wait between Redis ConnectionErrors, in seconds. | `1` |
| `MAX_PODS` | The maximum number of pods to scale up. Should be `1`. | `1` |
| `MIN_PODS` | The minimum number of pods to scale down. Should be `0`. | `0` |
| `KEYS_PER_POD` | The number of work keys per instance of `RESOURCE_NAME`. Should be `1`. | `1` |
