# autoscale.py
# Sort the key in the Redis base according to name and scale different
# Kubernetes deployments as appropriate.

import logging
import os
import re
import subprocess
import time
import sys

from redis import StrictRedis
from redis.exceptions import ConnectionError

class Autoscaler():
    def __init__(self):
        # initialize variables
        self.AUTOSCALING = os.environ['AUTOSCALING'].rstrip()
        self.REDIS_HOST = os.environ['REDIS_HOST']
        self.REDIS_PORT = os.environ['REDIS_PORT']
        self.sleep_seconds = int(os.environ['INTERVAL'])
        self.predict_keys = 0
        self.train_keys = 0

        # configure logger
        self._configure_logger()
        
        # establish Redis connection
        self.redis_client = StrictRedis(
            host=self.REDIS_HOST,
            port=self.REDIS_PORT,
            decode_responses=True,
            charset='utf-8')
   
        self._parse_autoscaling_variable()

    def _configure_logger(self):
        self.as_logger = logging.getLogger('autoscaler')
        self.as_logger.setLevel(logging.DEBUG)
        # Send logs to stdout so they can be read via Kubernetes.
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        self.as_logger.addHandler(sh)
        # Also send logs to a file for later inspection.
        fh = logging.FileHandler('autoscaler.log')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.as_logger.addHandler(fh)

    def _parse_autoscaling_variable(self):
        self.autoscaling_params = []
        autoscales = self.AUTOSCALING.split(';')
        for autoscale in autoscales:
            autoscale_list = autoscale.split("|")
            self.autoscaling_params.append(autoscale_list)

    def _make_kubectl_call(self, parameter_list):
        while True:
            try:
                subprocess.run(parameter_list)
                break
            except CalledProcessError as err:
                # For some reason, we can't execute this command right now.
                # Keep trying until we can.
                self._logger.warn("Trouble executing subprocess command " +
                        "using parameters %s. Retrying. %s: %s",
                        parameters_list, type(err).__name__, err)
                time.sleep(5)

    def _get_kubectl_output(self, parameter_list):
        while True:
            try:
                pods_info = subprocess.check_output(parameter_list)
                pods = pods_info.__str__()
                break
            except CalledProcessError as err:
                # For some reason, we can't execute this command right now.
                # Keep trying until we can.
                self._logger.warn("Trouble executing subprocess command " +
                        "using parameters %s. Retrying. %s: %s",
                        parameters_list, type(err).__name__, err)
                time.sleep(5)
        return pods

    def _redis_keys(self):
        self.as_logger.debug("Getting all Redis keys.")
        while True:
            try:
                self.all_redis_keys = self.redis_client.keys()
                break
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self.as_logger.warn("Trouble connecting to Redis. Retrying." +
                        " %s: %s", type(err).__name__, err)
                time.sleep(5)
        self.as_logger.debug("Got all Redis keys.")

    def _redis_hgetall(self, key):
        while True:
            try:
                key_values = self.redis_client.hgetall(key)
                break
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self.as_logger.warn("Trouble connecting to Redis. Retrying." +
                        " %s: %s", type(err).__name__, err)
                time.sleep(5)
        return key_values

    def _tally_keys(self):
        self.as_logger.debug("Tallying Redis keys.")
        self.predict_keys = 0
        self.train_keys = 0
        for key in self.all_redis_keys:
            if re.match('predict', key) or re.match('train', key):
                key_values = self._redis_hgetall(key)
                try:
                    if key_values['status'] not in ["done", "failed"]:
                        if re.match('predict', key):
                            self.predict_keys += 1
                        elif re.match('train', key):
                            self.train_keys += 1
                except KeyError:
                    self.as_logger.warn("Key " + key + " had no status field: "
                            + str(key_values))
        self.as_logger.debug("Found " + str(self.predict_keys) +
                " prediction keys and " + str(self.train_keys) +
                " training keys.")

    def _get_current_pods(self, namespace, resource_type, deployment):
        if resource_type == "deployment":
            pod_checking_keyword="desired"
        elif resource_type == "job":
            pod_checking_keyword="Completions"
        else:
            raise ValueError("The resource_type of " + str(resource_type) +
                    " is unsuitable. You need to use either " +
                    "\"deployment\" or \"job\".")
        parameter_list = ["kubectl", "-n", namespace, "describe",
                resource_type, deployment]
        deployment_info = self._get_kubectl_output(parameter_list)
        deployment_str = str(deployment_info)[2:-1]
        dstr = deployment_str.encode('utf-8').decode('unicode_escape')
        depl_info_list = dstr.split("\n")
        
        deployment_re = "Replicas:\s+([0-9]+) desired | [0-9]+ updated | " + \
                "[0-9]+ total | [0-9]+ available | [0-9]+ unavailable"
        job_re_one = "Completions:\s+([0-9]+)"
        for line in depl_info_list:
            if resource_type == "deployment":
                potential_match_deployment = re.match(deployment_re, line)
                if potential_match_deployment is not None:
                    current_pods = potential_match_deployment.group(1)
                    break
            elif resource_type == "job":
                potential_match_job_one = re.match(job_re_one, line)
                # This works so long as we don't delete Redis keys after
                # they've been procesed.
                # If we do start deleting keys (queue system), then we'll need
                # to also identify the "Succeeded" line and subtract that value
                # from Completions.
                if potential_match_job_one is not None:
                    current_pods = potential_match_job_one.group(1)
                    break
        if resource_type == "job" and current_pods == "<unset>":
            current_pods = 0
        current_pods = int(current_pods)
        return current_pods

    def _scale_deployments(self):
        # entry schema: minPods maxPods keysPerPod namespace resource_type 
        #               predict_or_train deployment
        self.as_logger.debug("Scaling deployments according to Redis key "
                + "tallies.")
        redis_keys_for_this_deployment = 0
        for entry in self.autoscaling_params:
            # parse entry
            try:
                min_pods = int(entry[0])
                max_pods = int(entry[1])
                keys_per_pod = int(entry[2])
                namespace = str(entry[3])
                resource_type = str(entry[4])
                predict_or_train = str(entry[5])
                deployment = str(entry[6])
            except IndexError:
                self.as_logger.error("Autoscaling entry " + str(entry) +
                        " is malformed.")
            self.as_logger.debug("    Scaling " + str(deployment))
            if predict_or_train == "predict":
                redis_keys_for_this_deployment = self.predict_keys
            elif predict_or_train == "train":
                redis_keys_for_this_deployment = self.train_keys
            else:
                raise ValueError("The value for predict_or_train from " +
                        str(entry) + " should be either \"predict\" or" +
                        " \"train\", not " + str(predict_or_train) + ".")

            # compute desired pods for this deployment
            desired_pods = redis_keys_for_this_deployment / keys_per_pod
            desired_pods = round(desired_pods - 0.5)
            if desired_pods > max_pods:
                desired_pods = max_pods
            if desired_pods < min_pods:
                desired_pods = min_pods

            # prevent intermediate scaledown
            # Since we can't direct the autoscaler to only remove idle pods,
            # we need to wait until all pods are idle to remove any of them.
            try:
                current_pods = self._get_current_pods(namespace, resource_type,
                        deployment)
            except ValueError as ex:
                raise ValueError("Error in autoscaling entry " + str(entry) +
                        " as follows: " + ex)
            if desired_pods < current_pods and desired_pods > 0:
                desired_pods = current_pods

            # scale pods, if necessary
            # turning off scaling for training-job until I figure out a
            # suitable method for scaling jobs
            if resource_type == "job":
                self.as_logger.debug("    Scaling has been disabled for jobs.")
            else:
                if desired_pods != current_pods:
                    parameter_list = ["kubectl", "scale", "-n", namespace,
                        "--replicas=" + str(desired_pods), resource_type +
                        "/" + deployment]
                    self._make_kubectl_call(parameter_list)
                    self.as_logger.debug("    Scaled " + deployment + " to " +
                            str(desired_pods) + " pods." )
                else:
                    self.as_logger.debug("    Deployment " + deployment + 
                            " stays at " + str(current_pods) + " pods." )

    def redis_monitoring_loop(self):
        self.as_logger.debug("Entering autoscaling loop.")
        while True:
            self._redis_keys()
            self._tally_keys()
            self._scale_deployments()
            self.as_logger.debug("Sleeping for " + str(self.sleep_seconds) +
                    " seconds.")
            time.sleep(self.sleep_seconds)
            self.as_logger.debug(" ")

if __name__=='__main__':
    autoscaler = Autoscaler()
    autoscaler.redis_monitoring_loop()
