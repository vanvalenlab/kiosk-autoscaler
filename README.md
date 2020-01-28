# kiosk-autoscaler

[![Build Status](https://travis-ci.com/vanvalenlab/kiosk-autoscaler.svg?branch=master)](https://travis-ci.com/vanvalenlab/kiosk-autoscaler)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-autoscaler/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-autoscaler?branch=master)

The `kiosk-autoscaler` turns on GPU resources when they are required and turns them back off when they are not needed, minimizing GPU costs for the DeepCell Kiosk.

## Configuration

The Autoscaler is configured using environment variables. Please find a table of all environment variables and their description below.

| Name | Description | Default Value |
| :---: | :---: | :---: |
| `AUTOSCALING` | A list of scaling configurations, associating certain GPU resources with Redis queues. | **REQUIRED.** |
| `INTERVAL` | How frequently the autoscaler checks for required resources, in seconds. | 5 |
| `QUEUES` | A `QUEUE_DELIMITER` separated list of work queues to monitor. | "predict,track" |
| `QUEUE_DELIMITER` | A string used to separate a list of queue names in `QUEUES`. | "," |
| `REDIS_HOST` | The IP address or hostname of Redis. | "redis-master" |
| `REDIS_PORT` | The port of Redis. | 6379 |
| `REDIS_INTERVAL` | Time to wait between Redis ConnectionErrors, in seconds. | 1 |
