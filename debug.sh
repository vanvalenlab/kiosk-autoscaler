#!/bin/bash

REDIS_HOST=redis-service
REDIS_PORT=40960
INTERVAL=5

while true; do
  queueMessagesJson=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT ping)
  echo $queueMessagesJson
  sleep $INTERVAL
done
