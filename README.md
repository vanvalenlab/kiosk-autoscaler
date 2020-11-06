# ![DeepCell Kiosk Banner](https://raw.githubusercontent.com/vanvalenlab/kiosk-console/master/docs/images/DeepCell_Kiosk_Banner.png)

[![Build Status](https://github.com/vanvalenlab/kiosk-autoscaler/workflows/build/badge.svg)](https://github.com/vanvalenlab/kiosk-autoscaler/actions)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-autoscaler/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-autoscaler?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](/LICENSE)

The `kiosk-autoscaler` turns on GPU resources when they are required and turns them back off when they are not needed, minimizing GPU costs for the DeepCell Kiosk.

This repository is part of the [DeepCell Kiosk](https://github.com/vanvalenlab/kiosk-console). More information about the Kiosk project is available through [Read the Docs](https://deepcell-kiosk.readthedocs.io/en/master) and our [FAQ](http://www.deepcell.org/faq) page.

## Configuration

The Autoscaler is configured using environment variables. Please find a table of all environment variables and their descriptions below.

| Name | Description | Default Value |
| :--- | :--- | :--- |
| `RESOURCE_NAME` | **REQUIRED**: The name of the resource to scale. | `""` |
| `RESOURCE_TYPE` | The resource type of `RESOURCE_NAME`, one of `"deployment"` or `"job"`. | `"deployment"` |
| `RESOURCE_NAMESPACE` | The Kubernetes namespace of `RESOURCE_NAME`. | `"default"` |
| `QUEUES` | A `QUEUE_DELIMITER` separated list of work queues to monitor. | `"predict,track"` |
| `QUEUE_DELIMITER` | A string used to separate a list of queue names in `QUEUES`. | `","` |
| `INTERVAL` | How frequently the autoscaler checks for required resources, in seconds. | `5` |
| `REDIS_HOST` | The IP address or hostname of Redis. | `"redis-master"` |
| `REDIS_PORT` | The port used to connect to Redis. | `6379` |
| `REDIS_INTERVAL` | Time to wait between Redis ConnectionErrors, in seconds. | `1` |
| `MAX_PODS` | The maximum number of pods to scale up. Should be `1`. | `1` |
| `MIN_PODS` | The minimum number of pods to scale down. Should be `0`. | `0` |
| `KEYS_PER_POD` | The number of work keys per instance of `RESOURCE_NAME`. Should be `1`. | `1` |

## Contribute

We welcome contributions to the [kiosk-console](https://github.com/vanvalenlab/kiosk-console) and its associated projects. If you are interested, please refer to our [Developer Documentation](https://deepcell-kiosk.readthedocs.io/en/master/DEVELOPER.html), [Code of Conduct](https://github.com/vanvalenlab/kiosk-console/blob/master/CODE_OF_CONDUCT.md) and [Contributing Guidelines](https://github.com/vanvalenlab/kiosk-console/blob/master/CONTRIBUTING.md).

## License

This software is license under a modified Apache-2.0 license. See [LICENSE](/LICENSE) for full  details.

## Copyright

Copyright © 2018-2020 [The Van Valen Lab](http://www.vanvalen.caltech.edu/) at the California Institute of Technology (Caltech), with support from the Paul Allen Family Foundation, Google, & National Institutes of Health (NIH) under Grant U24CA224309-01.
All rights reserved.
