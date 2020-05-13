import subprocess
import time
from datetime import datetime

import greenstalk
import requests
import json
import uuid
from statistics import mean
from collections import defaultdict

# Ranges between 22-50+
from stoppable_process import StoppableProcess

K8S_SERVICE_COUNT = 22

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "./scripts"
TERRAFORM_DIR = "./terraform/"

# set this to where-ever fs-kafka-k8s is cloned
# KAFKA_DEPLOY_DIR = "/home/nicholas/workspace/fs-kafka-k8s/"
KAFKA_DEPLOY_DIR = "/data/open-platform-checkouts/fs-kafka-k8s"
# PRODUCERS_CONSUMERS_DEPLOY_DIR = "/home/nicholas/workspace/fs-producer-consumer-k8s"
PRODUCERS_CONSUMERS_DEPLOY_DIR = "/data/open-platform-checkouts/fs-producer-consumer-k8s"
BURROW_DIR = "/data/open-platform-checkouts/fs-burrow-k8s"

DEFAULT_CONSUMER_TOLERANCE = 0.9
DEFAULT_THROUGHPUT_MB_S = 75
PRODUCER_STARTUP_INTERVAL_S = 26

# Cluster restarted: 13/05 @ 0951
SERVICE_ACCOUNT_EMAIL = "cluster-minimal-fae2058c0c52@kafka-k8s-trial.iam.gserviceaccount.com"

CLUSTER_NAME = "gke-kafka-cluster"
CLUSTER_ZONE = "europe-west2-a"

ENDPOINT_URL = "http://focussensors.duckdns.org:9000/consumer_reporting_endpoint"
stop_threads = False


class Controller:
    configurations = []

    def __init__(self, queue):
        self.consumer_throughput_queue = queue
        self.producer_increment_process = None
        self.consumer_throughput_process = None

    def post_json(self, endpoint_url, payload):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(endpoint_url, data=json.dumps(payload), headers=headers)

    def run(self):
        self.load_configurations()

        for configuration in self.configurations:
            print("\r\nTrying next configuration...")

            self.provision_node_pool(configuration)

            # only run if everything is ok
            if self.setup_configuration(configuration):
                # wait for input to run configuration
                input("Setup complete. Press any key to run the configuration...")
                self.run_configuration(configuration)
                self.consumer_throughput_process.join()

            # now teardown and unprovision
            self.teardown_configuration(configuration)

            # reset the stop threads flag
            global stop_threads
            stop_threads = False

    def k8s_delete_namespace(self, namespace):
        print(f"Deleting namespace: {namespace}")
        # run a script to delete a specific namespace
        filename = "./delete-namespace.sh"
        args = [filename, namespace]
        self.bash_command_with_output(args, SCRIPT_DIR)

    def flush_consumer_throughput_queue(self):
        # no flush() method exists in greenstalk so need to do it "brute force"
        print("Flushing consumer throughput queue")

        stats = self.consumer_throughput_queue.stats_tube("consumer_throughput")
        print(stats)

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

        print("Consumer throughput queue flushed.")

    def stop_threads(self):
        print("Stop threads called.")
        if self.producer_increment_process:
            self.producer_increment_process.stop()

        if self.consumer_throughput_process:
            self.consumer_throughput_process.stop()

    def teardown_configuration(self, configuration):
        print(f"\r\n4. Teardown configuration: {configuration}")

        # ensure threads are stopped
        self.stop_threads()

        # Remove producers & consumers
        self.k8s_delete_namespace(PRODUCER_CONSUMER_NAMESPACE)

        # Remove kafka brokers
        self.k8s_delete_namespace(KAFKA_NAMESPACE)

        # flush the consumer throughput queue
        self.flush_consumer_throughput_queue()

        # finally take down the kafka node pool
        self.unprovision_node_pool(configuration)

    # run a script to deploy kafka
    def k8s_deploy_kafka(self, num_partitions):
        print(f"Deploying Kafka and ZK...")
        filename = "./deploy.sh"
        args = [filename, str(num_partitions)]
        self.bash_command_with_wait(args, KAFKA_DEPLOY_DIR)

    # run a script to deploy producers/consumers
    def k8s_deploy_burrow(self):
        print(f"Deploying Burrow...")
        filename = "./deploy.sh"
        args = [filename]
        self.bash_command_with_wait(args, BURROW_DIR)

        # now wait for burrow external IP to be assigned
        print("Waiting 60s for Burrow external IP...")
        time.sleep(60)

    # run a script to deploy producers/consumers
    def k8s_deploy_producers_consumers(self):
        print(f"Deploying producers/consumers")
        filename = "./deploy/gcp/deploy.sh"
        args = [filename]
        self.bash_command_with_wait(args, PRODUCERS_CONSUMERS_DEPLOY_DIR)

    def k8s_scale_brokers(self, broker_count):
        print(f"Scaling brokers, broker_count={broker_count}")
        # run a script to start brokers
        filename = "./scale-brokers.sh"
        args = [filename, str(broker_count)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def k8s_configure_producers(self, start_producer_count, message_size):
        print(f"Configure producers, start_producer_count={start_producer_count}, message_size={message_size}")
        filename = "./configure-producers.sh"
        args = [filename, str(start_producer_count), str(message_size)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def k8s_scale_consumers(self, num_consumers):
        print(f"Configure consumers, num_consumers={num_consumers}")
        filename = "./scale-consumers.sh"
        args = [filename, str(num_consumers)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def check_k8s(self):
        print(f"Checking K8S...")
        filename = "./check_k8s.sh"
        args = [filename]
        output = self.bash_command_with_output(args, SCRIPT_DIR)
        service_count = int(output)
        print(f"k8s service count={service_count}")
        if service_count < K8S_SERVICE_COUNT:
            return False
        else:
            return True

    def get_broker_count(self):
        filename = "./get-brokers-count.sh"
        args = [filename]

        broker_count = 0
        try:
            broker_count = int(self.bash_command_with_output(args, SCRIPT_DIR))
        except ValueError:
            pass

        print(f"broker_count={broker_count}")
        return broker_count

    def check_brokers(self, expected_broker_count):
        return self.get_broker_count() == expected_broker_count

    def check_brokers_ok(self, configuration):
        i = 1
        attempts = configuration["number_of_brokers"] * 5
        check_brokers = self.check_brokers(configuration["number_of_brokers"])
        while not check_brokers:
            # allow 20s per broker 
            time.sleep(20)

            check_brokers = self.check_brokers(configuration["number_of_brokers"])
            print(f"Waiting for brokers to start...{i}/{attempts}")
            i += 1
            if i > attempts:
                print("Error: Time-out waiting for brokers to start.")
                return False

        print("Brokers started ok.")
        return True

    # run a script to configure gcloud
    def configure_gcloud(self, cluster_name, cluster_zone):
        print(f"configure_gcloud, cluster_name={cluster_name}, cluster_zone={cluster_zone}")
        filename = "./configure-gcloud.sh"
        args = [filename, cluster_name, cluster_zone]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def setup_configuration(self, configuration):
        print(f"\r\n2. Setup configuration: {configuration}")

        # configure gcloud (output is kubeconfig.yaml)
        self.configure_gcloud(CLUSTER_NAME, CLUSTER_ZONE)

        # check K8s & warn (rather than fail)
        all_ok = self.check_k8s()
        if not all_ok:
            print("Warning - Check K8S Services (fewer services running than expected).")

        # deploy kafka brokers
        # where num_partitions = num_consumers
        num_partitions = configuration["num_consumers"]
        self.k8s_deploy_kafka(num_partitions)

        # Configure # kafka brokers
        self.k8s_scale_brokers(str(configuration["number_of_brokers"]))

        all_ok = self.check_brokers_ok(configuration)
        if not all_ok:
            print("Aborting configuration - brokers not ok.")
            return False

        # deploy producers/consumers
        self.k8s_deploy_producers_consumers()

        # scale consumers
        self.k8s_scale_consumers(str(configuration["num_consumers"]))

        # deploy burrow
        self.k8s_deploy_burrow()

        # post configuration to the consumer reporting endpoint
        self.post_json(ENDPOINT_URL, configuration)

        return True

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, cwd=working_directory)
        p.wait()
        out = p.communicate()[0].decode("UTF-8")
        return out

    def bash_command_with_wait(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        try:
            subprocess.check_call(args, stderr=subprocess.STDOUT, cwd=working_directory)
        except subprocess.CalledProcessError as e:
            # There was an error - command exited with non-zero code
            print(e.output)
            return False

        return True

    def run_configuration(self, configuration):
        print(f"\r\n3. Running configuration: {configuration}")

        # Configure producers with required number of initial producers and their message size
        # Note - number of producers may be
        self.k8s_configure_producers(str(configuration["start_producer_count"]), str(configuration["message_size_kb"]))

        self.producer_increment_process = ProducerIncrementProcess(configuration)
        self.consumer_throughput_process = CheckConsumerThroughputProcess(configuration, self.consumer_throughput_queue)

        self.consumer_throughput_process.start()
        self.producer_increment_process.start()

    def load_configurations(self):
        print("Loading configurations.")

        configuration_uid = str(uuid.uuid4().hex.upper()[0:6])

        # configuration_3_750_n1_standard_1 = {
        configuration_template = {
            "configuration_uid": configuration_uid,
            "number_of_brokers": 3, "message_size_kb": 750, "start_producer_count": 1, "max_producer_count": 9,
            "num_consumers": 3,
            "producer_increment_interval_sec": 180, "machine_size": "n1-highmem-2", "disk_size": 100,
            "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5, "ignore_throughput_threshold": True}

        self.configurations.append(dict(configuration_template))
        # self.configurations.append(dict(configuration_template, message_size_kb=7500))

    def provision_node_pool(self, configuration):
        print(f"\r\n1. Provisioning node pool: {configuration}")

        filename = "./generate-kafka-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL, configuration["machine_size"], str(configuration["disk_type"]),
                str(configuration["disk_size"])]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        filename = "./generate-zk-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        filename = "./provision.sh"
        args = [filename]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        print("Node pool provisioned.")

    def unprovision_node_pool(self, configuration):
        print(f"\r\n5. Unprovisioning node pool: {configuration}")
        filename = "./unprovision.sh"
        args = [filename]
        self.bash_command_with_wait(args, TERRAFORM_DIR)
        print("Node pool unprovisioned.")


class CheckConsumerThroughputProcess(StoppableProcess):

    def __init__(self, configuration, queue):
        super().__init__()
        self.configuration = configuration
        self.consumer_throughput_queue = queue
        self.threshold_exceeded = 0

    def run(self):
        # create a dictionary of lists
        consumer_throughput_dict = defaultdict(list)

        # Ignore the first entry for all consumers
        consumer_dict = defaultdict()
        while len(consumer_dict.keys()) < int(self.configuration["num_consumers"]):
            try:
                job = self.consumer_throughput_queue.reserve(timeout=1)
                if job is None:
                    continue

                data = json.loads(job.body)
                print(f"Ignoring data {data} on consumer throughput queue...")
                consumer_dict[data["consumer_id"]] = 1

                # Delete from queue
                try:
                    self.consumer_throughput_queue.delete(job)
                except OSError as e:
                    print(f"Warning: unable to delete job {job}, {e}", e)
            except greenstalk.TimedOutError:
                print("Timed out waiting for initial data on consumer throughput queue.")
                pass

        while not self.is_stopped():
            try:
                job = self.consumer_throughput_queue.reserve(timeout=1)

                if job is None:
                    print("Nothing on consumer throughput queue...")
                    # sleep for 10ms
                    time.sleep(.10)
                    continue

                data = json.loads(job.body)

                print(f"Received data {data} on consumer throughput queue.")

                consumer_id = data["consumer_id"]
                throughput = data["throughput"]
                num_producers = data["producer_count"]

                if not self.configuration["ignore_throughput_threshold"]:
                    # append to specific list (as stored in dict)
                    consumer_throughput_dict[consumer_id].append(throughput)

                    if len(consumer_throughput_dict[consumer_id]) >= 10:
                        # truncate list to last 10 entries
                        consumer_throughput_dict[consumer_id] = consumer_throughput_dict[consumer_id][-10:]

                        consumer_throughput_average = mean(consumer_throughput_dict[consumer_id])
                        print(f"Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                        consumer_throughput_tolerance = (
                                DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)
                        if consumer_throughput_average < consumer_throughput_tolerance:
                            print(
                                f"Warning: Consumer {consumer_id} throughput average {consumer_throughput_average} is below tolerance {consumer_throughput_tolerance}")
                            self.threshold_exceeded += 1

                            # clear the list of entries for the given consumer
                            consumer_throughput_dict[consumer_id].clear()

                            if self.threshold_exceeded == 3:
                                print("Stopping after 3 threshold events...")
                                self.stop()

                # Finally delete from queue
                try:
                    self.consumer_throughput_queue.delete(job)
                except OSError as e:
                    print(f"Warning: unable to delete job {job}, {e}", e)

            except greenstalk.TimedOutError:
                print("Warning: nothing in consumer throughput queue.")
            except greenstalk.UnknownResponseError:
                print("Warning: unknown response from beanstalkd server.")
            except greenstalk.DeadlineSoonError:
                print("Warning: job timeout in next second.")
            except ConnectionError as ce:
                print(f"Error: ConnectionError: {ce}")


class ProducerIncrementProcess(StoppableProcess):

    def __init__(self, configuration):
        super().__init__()
        self.configuration = configuration

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, cwd=working_directory)
        p.wait()
        out = p.communicate()[0].decode("UTF-8")
        return out

    def k8s_scale_producers(self, producer_count):
        filename = "./scale-producers.sh"
        args = [filename, str(producer_count)]
        self.bash_command_with_output(args, SCRIPT_DIR)

    def get_producer_count(self):
        filename = "./get-producers-count.sh"
        args = [filename]

        producer_count = 0
        try:
            producer_count = int(self.bash_command_with_output(args, SCRIPT_DIR))
        except ValueError:
            pass

        print(f"reported_producer_count={producer_count}")
        return producer_count

    def check_producers(self, expected_producer_count):
        actual_producer_count = self.get_producer_count()
        print(f"actual_producer_count={actual_producer_count}, expected_producer_count={expected_producer_count}")
        if actual_producer_count == expected_producer_count:
            return True
        else:
            return False

    def run(self):
        first_producer = True

        # start at configured value
        actual_producer_count = self.configuration["start_producer_count"]
        while not self.is_stopped() and (actual_producer_count <= self.configuration["max_producer_count"]):
            if not first_producer:
                # Wait for a specified interval before starting the next producer
                producer_increment_interval_sec = self.configuration["producer_increment_interval_sec"]
                print(f"Waiting {producer_increment_interval_sec}s before starting next producer.")
                time.sleep(producer_increment_interval_sec)
            else:
                first_producer = False

            print(f"Starting producer {actual_producer_count}")

            # Start a new producer
            self.k8s_scale_producers(str(actual_producer_count))

            time.sleep(PRODUCER_STARTUP_INTERVAL_S)

            i = 1
            check_producers = self.check_producers(actual_producer_count)
            while not self.is_stopped() and not check_producers:
                time.sleep(5)
                check_producers = self.check_producers(actual_producer_count)
                print(f"(Still) waiting for producer to start... ({i}/24)")
                i += 1
                if i > 24:
                    print("Error: Timeout waiting for producer to start...")
                    self.stop()

            actual_producer_count += 1


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    print("Reminder: have you remembered to update the SERVICE_ACCOUNT_EMAIL (if cluster has been bounced?)")
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = Controller(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
