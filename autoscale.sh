#!/bin/bash

DEBUG=${DEBUG:-true}
INTERVAL=${INTERVAL:-5}
REDIS_HOST=${REDIS_HOST:-redis-service}
REDIS_PORT=${REDIS_PORT:-40960}
#namespace=${NAMESPACE:-deepcell}
#deployment=${DEPLOYMENT}
#keysPerPod=5
#minPods=${MIN_PODS:-0}
#maxPods=${MAX_PODS:-4}

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
    #debug "$(date) -- debug -- $(kubectl -n $namespace describe $resource_type $deployment | grep 'Parallelism')"
    current=$(kubectl -n $namespace describe $resource_type $deployment | \
      grep "$pod_checking_keyword" | awk '{print $2}' | head -n1)

    if [[ $current != "" ]]; then
      #debug "$(date) -- debug -- $(echo $current)"
      echo $current
      return 0
    fi

    sleep 3
  done

  echo ""
}



autoscalingNoWS=$(echo "$AUTOSCALING" | tr -d "[:space:]")
IFS=';' read -ra autoscalingArr <<< "$autoscalingNoWS"
debug "$(date) -- debug -- autoscalingNoWS: $autoscalingNoWS"
debug "$(date) -- debug -- autoscalingArr[0]: ${autoscalingArr[0]}"
debug "$(date) -- debug -- autoscalingArr[1]: ${autoscalingArr[1]}"

#
# Main loop
#
while true; do

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
  
      # find out how many keys we retrieved
      numberOfKeys=0
      for key in $queueKeys
      do
          ((numberOfKeys++))
      done
  
      # and determine how many pods we need.
      requiredPods=$(echo "$numberOfKeys/$keysPerPod" | bc 2> /dev/null)
  
      # If we don't have enough jobs to fill up an entire pod, should we still provision one?
      # Yes, if we're talking about prediction pods.
      if [[ $predict_or_train == "predict" && $requiredPods -eq 0  &&  $numberOfKeys -gt 0 ]]; then
        requiredPods=1
      fi
  
      debug "$(date) -- debug -- namespace: $namespace"
      debug "$(date) -- debug -- resource type: $resource_type"
      debug "$(date) -- debug -- predict or train: $predict_or_train"
      debug "$(date) -- debug -- deployment name: $deployment"
      debug "$(date) -- debug -- number of keys: $numberOfKeys"
      debug "$(date) -- debug -- number of keys per pod: $keysPerPod"
      debug "$(date) -- debug -- number of required pods: $requiredPods"
      debug "$(date) -- debug -- max pods: $maxPods"
      debug "$(date) -- debug -- min pods: $minPods"
      # Now, if we need one or more pods
      if [[ $requiredPods -ge 1 ]]; then
  
        # find out how many pods we've already requested.
        currentPods=$(getCurrentPods)
        debug "$(date) -- debug -- current number of pods: $currentPods"
  
        # If we alrady have some pods requested
        if [[ $currentPods != "" ]]; then
          # and the amount we need is different from what we already have requested
          if [[ "$requiredPods" -ne "$currentPods" ]]; then
              debug "$(date) -- debug -- need more pods"
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
  
            # If we are not constrained by scaling limits, then go ahead and scale.
            if [[ $scale -eq 0 ]]; then
              # To slow down the scale-down policy, scale down in steps (reduce 10% on every iteration)
              if [[ $desiredPods -lt $currentPods ]]; then
                desiredPods=$(awk "BEGIN { print int( ($currentPods - $desiredPods) * 0.9 + $desiredPods ) }")
              fi
  
              kubectl scale -n $namespace --replicas=$desiredPods $resource_type/$deployment 1> /dev/null
  
              if [[ $? -eq 0 ]]; then
                # Adjust logging and Slack notifications based on LOGS env and desiredPods number
                log=false
                avgPods=$(awk "BEGIN { print int( ($minPods + $maxPods) / 2 ) }")
  
                if [[ $LOGS == "HIGH" ]]; then
                  log=true
                elif [[ $LOGS == "MEDIUM" && ($desiredPods -eq $minPods || $desiredPods -eq $avgPods || $desiredPods -eq $maxPods) ]]; then
                  log=true
                elif [[ $LOGS == "LOW" && ($desiredPods -eq $minPods || $desiredPods -eq $maxPods) ]]; then
                  log=true
                fi
  
                if $log ; then
                  echo "$(date) -- Scaled $deployment to $desiredPods pods ($queueMessages msg in the Redis queue)"
                fi
              else
                echo "$(date) -- Failed to scale $deployment pods."
              fi
            fi
          else
            debug "$(date) -- debug -- apparently don't need more pods"
          fi
        else
          echo "$(date) -- Failed to get current pods number for $deployment."
        fi
      else
        echo "$(date) -- Don't need any pods for $deployment."
        if [[ $minPods -eq 0 ]]; then
          desiredPods=$requiredPods
          kubectl scale -n $namespace --replicas=$desiredPods $resource_type/$deployment 1> /dev/null
          echo "$(date) -- So we scaled down to 0."
        else
          echo "$(date) -- But we have to keep a minimum number of pods."
        fi
      fi
    else
      echo "$(date) -- Failed to get queue messages from $RABBIT_HOST for $deployment."
    fi
    debug "$(date) -- debug --"
    debug "$(date) -- debug --"
    debug "$(date) -- debug --"
    debug "$(date) -- debug --"
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
