#!/bin/bash

function initialize_parameters() {
  DEBUG=${DEBUG:-true}
  INTERVAL=${INTERVAL:-5}
  REDIS_HOST=${REDIS_HOST:-redis-service}
  REDIS_PORT=${REDIS_PORT:-40960}
  #namespace=${NAMESPACE:-deepcell}
  #deployment=${DEPLOYMENT}
  #keysPerPod=5
  #minPods=${MIN_PODS:-0}
  #maxPods=${MAX_PODS:-4}
  OLD_VERSION_HASH=$(kubectl version | sha1sum )
}

function parse_parameters() {
  autoscalingNoWS=$(echo "$AUTOSCALING" | tr -d "[:space:]")
  IFS=';' read -ra autoscalingArr <<< "$autoscalingNoWS"
  debug "$(date) -- debug -- autoscalingNoWS: $autoscalingNoWS"
  debug "$(date) -- debug -- autoscalingArr[0]: ${autoscalingArr[0]}"
  debug "$(date) -- debug -- autoscalingArr[1]: ${autoscalingArr[1]}"
}

function debug() {
  if [ "${DEBUG}" == "true" ]; then
    echo $*
  fi
}

function getCurrentPods() {
  # Retry up to 5 times if kubectl fails
  for i in $(seq 5); do
    if [ "$resource_type" == "deployment" ]; then
        pod_checking_keyword=desired
    elif [ "$resource_type" == "job" ]; then
        pod_checking_keyword=Parallelism
    fi
    current=$(kubectl -n $namespace describe $resource_type $deployment | \
      grep "$pod_checking_keyword" | awk '{print $2}' | head -n1)
    echo $current
    return 0

    sleep 3
  done

  echo ""
}

function verify_mins_and_maxes() {
  # For jobs, we may not want to have a maxPods.
  # This can be denoted by setting maxPods=none.
  if [[ "$maxPods" == "none" ]]; then
    maxPods=((requiredPods+currentPods+1))
    echo "Chosen max for this job: $maxPods"
  fi

  # Determine how many pods we need, taking into account scaling limits.
  desiredPods=""
  # Flag used to prevent scaling down or up if currentPods are already min or max respectively.
  scale=0
  if [[ $requiredPods -le $minPods ]]; then
    desiredPods=$minPods
    # If currentPods are already at min, do not scale down
    if [[ $currentPods -eq $minPods ]]; then
      scale=1
    fi
  elif [[ $requiredPods -ge $maxPods ]]; then
    desiredPods=$maxPods
    # If currentPods are already at max, do not scale up
    if [[ $currentPods -eq $maxPods ]]; then
      scale=1
    fi
  else
    desiredPods=$requiredPods
  fi
}

function prevent_intermediate_scaledown() {
  # We're also inserting a check to prevent scaling down until we only want zero pods.
  if [[ $desiredPods -le $currentPods ]]; then
    if [[ $desiredPods -eq 0 ]]; then
      #debug "$(date) -- debug -- preventing scaledown to zero"
      desiredPods=$currentPods
    else
      #debug "$(date) -- debug -- preventing intermediate scaledown"
      desiredPods=$currentPods
    fi
  fi
}  

function scale_and_log() {
  if [[ $scale -eq 0 ]]; then
    if [[ "$desiredPods" -ne "$currentPods" ]]; then
      output_debug_info
      scale_pods
    fi
  fi
}

function scale_pods() {
  kubectl scale -n $namespace --replicas=$desiredPods $resource_type/$deployment 1> /dev/null
  debug "$(date) -- debug -- scaled to $desiredPods pods"
}

function get_keys() {
  # TODO: resolve ambiguity between zipkeys and trainkeys (both use .zip files)
  # find out how many of the keys we retrieved were zip files, how many were
  # images, and how many were, regardless of their file type, expired
  imageKeys=0
  zipKeys=0
  for key in $queueKeys
  do
    key_status=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT hget $key status)
    if [[ "$key_status" != "done" && "$key_status" != "failed" ]]; then
      file_name=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT hget $key file_name)
      if [[ $file_name =~ ^.+\.zip$ ]]; then
        ((zipKeys++))
      else
        ((imageKeys++))
      fi
    fi
  done
  #debug "$(date) -- debug -- zipkeys: $zipKeys"
  #debug "$(date) -- debug -- imagekeys: $imageKeys"
}

function determine_required_pods() {
  # and determine how many pods we need.
  imagePods=$(echo "$imageKeys/$keysPerPod" | bc 2> /dev/null)
  zipPods=$(echo "$zipKeys/$keysPerPod" | bc 2> /dev/null)

  # Does this deployment revolve around zip files or image files?
  # Note that the answer could vary for prediction-related deployments, but that 
  # all training-related deployments use zip files.
  if [ "$predict_or_train" == "predict" ]; then
    if [[ "$deployment" == "zip-consumer-deployment" ]]; then
      requiredPods=$zipPods
    else
      requiredPods=$imagePods
    fi
  else
    requiredPods=$zipPods
  fi

  # If we don't have enough jobs to fill up an entire pod, should we still provision one?
  # Yes, if we're talking about image prediction pods.
  if [[ $predict_or_train == "predict" && $requiredPods -eq 0  &&  $imageKeys -gt 0 ]]; then
    requiredPods=1
  fi
}

function output_debug_info() {
  #debug "$(date) -- debug -- namespace: $namespace"
  #debug "$(date) -- debug -- resource type: $resource_type"
  #debug "$(date) -- debug -- predict or train: $predict_or_train"
  debug "$(date) -- debug --"
  debug "$(date) -- debug -- deployment name: $deployment"
  if [[ "$deployment" == "zip-consumer-deployment" ]]; then
    debug "$(date) -- debug -- number of keys: $zipKeys"
  else
    debug "$(date) -- debug -- number of keys: $imageKeys"
  fi 
  debug "$(date) -- debug -- number of keys per pod: $keysPerPod"
  debug "$(date) -- debug -- number of required pods: $requiredPods"
  debug "$(date) -- debug -- max pods: $maxPods"
  debug "$(date) -- debug -- min pods: $minPods"
  debug "$(date) -- debug -- current pods: $currentPods"
}

function adjust_pods() {
  # find out how many pods we've already requested.
  currentPods=$(getCurrentPods)
  # If we already have some pods requested
  if [[ $currentPods != "" ]]; then
    # and the amount we need is different from what we already have requested
    if [[ "$requiredPods" -ne "$currentPods" ]]; then
      # Determine how many pods we need, taking into account scaling limits.
      verify_mins_and_maxes
      # For the time being, let's prevent scaledown, unless it's a complete scaledown
      prevent_intermediate_scaledown
      # If appropriate, go ahead and scale.
      scale_and_log
    fi
  else
    echo "$(date) -- Failed to get current pods number for $deployment."
  fi
}

#
# Main loop
#
function main_loop() {
while true; do

  # check cluster version
  # if cluster has been upgraded recently, let's not touch the pod replica numbers for a minute
  # in order to allow the Redis database time to fully reload
  NEW_VERSION_GREP_TIMEOUT=$(kubectl version | grep timeout)
  NEW_VERSION_GREP_REFUSED=$(kubectl version | grep refused)
  # does cluster show keywords indicative of a broken connection to the master node?
  if [ -z "$NEW_VERSION_GREP_TIMEOUT" ]; then
    if [ -z "$NEW_VERSION_GREP_REFUSED" ]; then
      # we are connected to master, so...
      echo "We appear to be connected to master."
      NEW_VERSION_HASH=$(kubectl version | sha1sum)
      if [ "$OLD_VERSION_HASH" == "$NEW_VERSION_HASH" ]; then
        echo "Cluster version hasn't changed."
      else
        echo "Cluster version appears to have changed. Sleeping for 15 minutes."
        sleep 900
      fi
      OLD_VERSION_HASH=${NEW_VERSION_HASH}
    else
      echo "Version checking encountered the word 'refused'!"
    fi
  else
    echo "Version checking encountered the word 'timeout'!"
  fi

  for autoscaler in "${autoscalingArr[@]}"; do
    IFS='|' read minPods maxPods keysPerPod namespace resource_type predict_or_train deployment <<< "$autoscaler"
    # the "resource_type" field is meant to indicate whether a given resource is a deployment or a job
    # the "predict_or_train" field is meant to indicate whether a given deployment deals with training or prediction

    # Retrieve all keys
    if [ "$predict_or_train" == "predict" ]; then
      queueKeys=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT keys "predict_*")
    elif [ "$predict_or_train" == "train" ]; then
      queueKeys=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT keys "train_*")
    fi
  
    # then, if the last call was successful
    if [[ $? -eq 0 ]]; then
      get_keys
      determine_required_pods
      adjust_pods
    else
      echo "$(date) -- Failed to get entries from Redis for $deployment."
    fi
    #debug "$(date) -- debug --"
  done

  # We need to account for the long time it takes to start up a GPU instance.
  # A crude way of doing this is just to greatly lengthen the queue-checking
  # interval when GPUs are requested.
  if [[ $desiredPods -gt 0 ]]; then
    ADJUSTED_INTERVAL=360
  else
    ADJUSTED_INTERVAL=$INTERVAL
  fi

  sleep $INTERVAL
done
}

### Code execution begins here. ###
initialize_parameters
parse_parameters
main_loop
