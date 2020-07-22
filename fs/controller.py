import logging
import subprocess
import time
import os
from datetime import datetime

import greenstalk
import requests
import json
import uuid
from fs.stress_test_process import StressTestProcess
from fs.soak_test_process import SoakTestProcess
from fs.utils import SCRIPT_DIR, PRODUCER_CONSUMER_NAMESPACE, KAFKA_NAMESPACE, KAFKA_DEPLOY_DIR, BURROW_DIR, \
    PRODUCERS_CONSUMERS_DEPLOY_DIR, CLUSTER_NAME, CLUSTER_ZONE, TERRAFORM_DIR, SERVICE_ACCOUNT_EMAIL, \
    ENDPOINT_URL, addlogger

stop_threads = False


@addlogger
class Controller:

    def __init__(self, queue):
        self.configurations = []

        self.consumer_throughput_queue = queue
        self.stress_test_process = None
        self.soak_test_process = None

        # template configuration
        # 5 brokers, 3 ZK
        self.configuration_template = {"number_of_brokers": 5, "message_size_kb": 750, "start_producer_count": 1,
                                  "max_producer_count": 16, "num_consumers": 1, "producer_increment_interval_sec": 60,
                                  "machine_size": "n1-standard-8", "disk_size": 100, "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5,
                                  "ignore_throughput_threshold": False, "teardown_broker_nodes": False, "replication_factor": 1, "num_zk": 3}

        # default the number of partitions
        self.configuration_template["number_of_partitions"] = self.configuration_template["number_of_brokers"] * 3

        self.run_uid = self.get_run_uid()

        self.configure_logging()

    def configure_logging(self):
        # log directory
        base_directory = os.path.dirname(os.path.abspath(__file__))
        now = datetime.now()
        path = os.path.join(base_directory, "..", "log", now.strftime("%Y-%m-%d"))

        if not os.path.exists(path):
            os.makedirs(path)

        log_formatter = logging.Formatter("%(asctime)s [%(name)s] [%(levelname)-5.5s]  %(message)s")
        root_logger = logging.getLogger("fs")
        root_logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler("{0}/{1}.log".format(path, self.run_uid))
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

    def post_json(self, endpoint_url, payload):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(endpoint_url, data=json.dumps(payload), headers=headers)

    def run(self):
        self.load_configurations()

        for configuration in self.configurations:
            configuration_as_dict = configuration[0]

            self.__log.info(f"Next configuration: {configuration_as_dict}")

            self.provision_node_pool(configuration_as_dict)

            # only run if everything is ok
            if self.setup_configuration(configuration_as_dict):
                # wait for input to run configuration
                # input("Setup complete. Press any key to run the configuration...")
                self.run_configuration(configuration_as_dict)

            # now teardown and unprovision
            self.teardown_configuration(configuration_as_dict)

            # reset the stop threads flag
            global stop_threads
            stop_threads = False

    def k8s_delete_namespace(self, namespace):
        self.__log.info(f"Deleting namespace: {namespace}")
        # run a script to delete a specific namespace
        filename = "./delete-namespace.sh"
        args = [filename, namespace]
        self.bash_command_with_output(args, SCRIPT_DIR)

    def flush_consumer_throughput_queue(self):
        # no flush() method exists in greenstalk so need to do it "brute force"
        self.__log.info("Flushing consumer throughput queue")

        stats = self.consumer_throughput_queue.stats_tube("consumer_throughput")
        self.__log.info(stats)

        done = False
        while not done:
            job = None
            try:
                job = self.consumer_throughput_queue.reserve(timeout=0)
            except greenstalk.TimedOutError:
                pass

            if job is None:
                done = True
            else:
                self.consumer_throughput_queue.delete(job)

        self.__log.info("Consumer throughput queue flushed.")

    def stop_threads(self):
        self.__log.info("Stop threads called.")

        if self.stress_test_process:
            self.stress_test_process.stop()

        if self.soak_test_process:
            self.soak_test_process.stop()

    def teardown_configuration(self, configuration):
        self.__log.info(f"4. Teardown configuration: {configuration}")

        # ensure threads are stopped
        self.stop_threads()

        # Remove producers & consumers
        self.k8s_delete_namespace(PRODUCER_CONSUMER_NAMESPACE)

        # Remove kafka brokers & ZK
        self.k8s_delete_namespace(KAFKA_NAMESPACE)

        # flush the consumer throughput queue
        self.flush_consumer_throughput_queue()

        # finally take down the kafka node pool
        if configuration["teardown_broker_nodes"]:
          self.unprovision_node_pool(configuration)
        else:
          self.__log.info("Broker nodes left standing.")

    def k8s_deploy_zk(self):
        """
        run a script to deploy ZK
        :return:
        """
        self.__log.info(f"Deploying ZK...")

        filename = "./deploy-zk.sh"
        args = [filename]
        self.bash_command_with_wait(args, KAFKA_DEPLOY_DIR)

    def k8s_deploy_kafka(self, num_partitions, replication_factor):
        """
        # run a script to deploy kafka

        :param num_partitions:
        :param replication_factor:
        :return:
        """
        self.__log.info(f"Deploying Kafka with {num_partitions} partitions, replication factor {replication_factor}...")
        filename = "./deploy-kafka.sh"
        args = [filename, str(num_partitions), str(replication_factor)]
        self.bash_command_with_wait(args, KAFKA_DEPLOY_DIR)

    def get_burrow_ip(self):
        filename = "./get-burrow-external-ip.sh"
        args = [filename]
        try:
            burrow_ip = self.bash_command_with_output(args, SCRIPT_DIR)
        except ValueError:
            pass

        return burrow_ip

    # run a script to deploy producers/consumers
    def k8s_deploy_burrow(self):
        self.__log.info(f"Deploying Burrow...")
        filename = "./deploy.sh"
        args = [filename]
        self.bash_command_with_wait(args, BURROW_DIR)

        # wait for burrow external IP to be assigned
        self.__log.info("Waiting for Burrow external IP...")
        burrow_ip = self.get_burrow_ip()
        while burrow_ip is None or burrow_ip == "":
            time.sleep(5)
            burrow_ip = self.get_burrow_ip()

        self.__log.info(f"Burrow external IP: {burrow_ip}")

    # run a script to deploy producers/consumers
    def k8s_deploy_producers_consumers(self):
        self.__log.info(f"Deploying producers/consumers")
        filename = "./deploy/gcp/deploy.sh"
        args = [filename]
        self.bash_command_with_wait(args, PRODUCERS_CONSUMERS_DEPLOY_DIR)

    def k8s_scale_brokers(self, broker_count):
        self.__log.info(f"Scaling brokers, broker_count={broker_count}")
        # run a script to start brokers
        filename = "./scale-brokers.sh"
        args = [filename, str(broker_count)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def k8s_configure_producers(self, start_producer_count, message_size):
        self.__log.info(f"Configure producers, start_producer_count={start_producer_count}, message_size={message_size}")
        filename = "./configure-producers.sh"
        args = [filename, str(start_producer_count), str(message_size)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def k8s_scale_consumers(self, num_consumers):
        self.__log.info(f"Configure consumers, num_consumers={num_consumers}")
        filename = "./scale-consumers.sh"
        args = [filename, str(num_consumers)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def get_zookeepers_count(self):
        filename = "./get-zookeepers-count.sh"
        args = [filename]

        zk_count = 0
        try:
            zk_count = int(self.bash_command_with_output(args, SCRIPT_DIR))
        except ValueError:
            pass

        # self.__log.info(f"zk={zk_count}")
        return zk_count

    def get_broker_count(self):
        filename = "./get-brokers-count.sh"
        args = [filename]

        broker_count = 0
        try:
            broker_count = int(self.bash_command_with_output(args, SCRIPT_DIR))
        except ValueError:
            pass

        # self.__log.info(f"broker_count={broker_count}")
        return broker_count

    def check_brokers(self, expected_broker_count):
        return self.get_broker_count() == expected_broker_count

    def check_zookeepers(self, expected_zk_count):
        return self.get_zookeepers_count() == expected_zk_count

    def check_zk_ok(self, configuration):
        # allow 46s per ZK
        WAIT_INTERVAL = 10
        num_zk = configuration["num_zk"]
        attempts = (45 * num_zk) / WAIT_INTERVAL

        check_zks = self.check_zookeepers(num_zk)
        i = 1
        while not check_zks:
            time.sleep(WAIT_INTERVAL)

            check_zks = self.check_zookeepers(num_zk)
            self.__log.info(f"Waiting for zks to start...{i}/{attempts}")
            i += 1
            if i > attempts:
                self.__log.error("Time-out waiting for zks to start.")
                return False

        self.__log.info("ZKs started ok.")
        return True

    def check_brokers_ok(self, configuration):
        # allow 30s per broker
        WAIT_INTERVAL = 10
        num_brokers = configuration["number_of_brokers"]
        attempts = (45 * num_brokers) / WAIT_INTERVAL

        check_brokers = self.check_brokers(num_brokers)
        i = 1
        while not check_brokers:
            time.sleep(WAIT_INTERVAL)

            check_brokers = self.check_brokers(num_brokers)
            self.__log.info(f"Waiting for brokers to start...{i}/{attempts}")
            i += 1
            if i > attempts:
                self.__log.error("Time-out waiting for brokers to start.")
                return False

        self.__log.info("Brokers started ok.")
        return True

    # run a script to configure gcloud
    def configure_gcloud(self, cluster_name, cluster_zone):
        # self.__log.info(f"configure_gcloud, cluster_name={cluster_name}, cluster_zone={cluster_zone}")
        filename = "./configure-gcloud.sh"
        args = [filename, cluster_name, cluster_zone]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def setup_configuration(self, configuration):
        self.__log.info(f"2. Setup configuration: {configuration}")

        # configure gcloud (output is kubeconfig.yaml)
        self.configure_gcloud(CLUSTER_NAME, CLUSTER_ZONE)

        # Note - hard-coded to 3 ZK in zookeeper-3.4.14/zookeeper-statefulset.yaml
        self.k8s_deploy_zk()

        all_ok = self.check_zk_ok(configuration)
        if not all_ok:
            self.__log.info("Aborting configuration - ZK not ok.")
            return False

        # deploy kafka brokers
        # where num_partitions = max(#P, #C), where #P = TT / 75)
        # see https://docs.cloudera.com/runtime/7.1.0/kafka-performance-tuning/topics/kafka-tune-sizing-partition-number.html
        num_partitions = configuration["number_of_partitions"]
        replication_factor = configuration["replication_factor"]
        self.k8s_deploy_kafka(num_partitions, replication_factor)

        # Configure # kafka brokers
        self.k8s_scale_brokers(str(configuration["number_of_brokers"]))

        all_ok = self.check_brokers_ok(configuration)
        if not all_ok:
            self.__log.info("Aborting configuration - brokers not ok.")
            return False

        # deploy producers/consumers
        self.k8s_deploy_producers_consumers()

        # deploy burrow
        self.k8s_deploy_burrow()

        # scale consumers
        self.k8s_scale_consumers(str(configuration["num_consumers"]))

        # wait 5s for each consumer to start
        time.sleep(5*configuration["num_consumers"])

        # post configuration to the consumer reporting endpoint
        self.post_json(ENDPOINT_URL, configuration)

        return True

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        # self.__log.info(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, cwd=working_directory)
        p.wait()
        out = p.communicate()[0].decode("UTF-8")
        return out

    def bash_command_with_wait(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        # self.__log.info(args)
        try:
            subprocess.check_call(args, stderr=subprocess.STDOUT, cwd=working_directory)
        except subprocess.CalledProcessError as e:
            # There was an error - command exited with non-zero code
            self.__log.error(f"{e.output}")
            return False

        return True

    def run_stress_test(self, configuration, queue):
        self.__log.info(f"3. Running stress test.")

        self.stress_test_process = StressTestProcess(configuration, queue)
        self.stress_test_process.start()

        # wait for thread to exit
        self.stress_test_process.join()

        self.__log.info(f"3. Stress test completed.")

    def run_soak_test(self, configuration, queue):
        self.__log.info(f"4. Running soak test.")

        self.soak_test_process = SoakTestProcess(configuration, queue)

        # start the thread for soak test
        self.soak_test_process.start()

        # wait for thread to exit
        self.soak_test_process.join()

        self.__log.info(f"3. Soak test completed.")

    def run_configuration(self, configuration):
        self.__log.info(f"3. Running configuration: {configuration}")

        # Configure producers with required number of initial producers and their message size
        # Note - number of producers may be greater than 0
        self.k8s_configure_producers(str(configuration["start_producer_count"]), str(configuration["message_size_kb"]))

        # run stress test
        self.run_stress_test(configuration, self.consumer_throughput_queue)

        # run soak test
        self.run_soak_test(configuration, self.consumer_throughput_queue)

    def get_uid(self):
        return str(uuid.uuid4().hex.upper()[0:6])

    def get_run_uid(self):
        return "run_" + self.get_uid()

    def load_configurations(self):
        raise NotImplementedError("Please use a sub-class to implement actual configurations")

    def get_configuration_description(self):
        raise NotImplementedError("Please use a sub-class to implement the configuration description")

    def get_configurations(self, template):
        configurations = []

        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "start_producer_count": 9}
        configurations.append(dict(template, **d))

        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "num_consumers": 2, "start_producer_count": 8}
        configurations.append(dict(template, **d))

        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "num_consumers": 3, "start_producer_count": 7}
        configurations.append(dict(template, **d))

        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "num_consumers": 4, "start_producer_count": 4}
        configurations.append(dict(template, **d))

        # start_producer_count defaults to 1
        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "num_consumers": 5, "max_producer_count": 14}
        configurations.append(dict(template, **d))

        # start_producer_count defaults to 1
        # Final configuration should bring down the nodes
        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "num_consumers": 6, "max_producer_count": 12,
             "teardown_broker_nodes": True}
        configurations.append(dict(template, **d))

        return configurations

    def provision_node_pool(self, configuration):
        self.__log.info("1. Provisioning node pool.")

        filename = "./generate-kafka-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL, configuration["machine_size"], configuration["disk_type"], str(configuration["disk_size"]), str(configuration["number_of_brokers"])]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        filename = "./generate-zk-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        filename = "./provision.sh"
        args = [filename]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        self.__log.info("Node pool provisioned.")

    def unprovision_node_pool(self, configuration):
        self.__log.info(f"5. Unprovisioning node pool: {configuration}")
        filename = "./unprovision.sh"
        args = [filename]
        self.bash_command_with_wait(args, TERRAFORM_DIR)
        self.__log.info("Node pool unprovisioned.")


