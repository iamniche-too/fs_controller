import subprocess
import time
from datetime import datetime

import threading
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

DEFAULT_CONSUMER_TOLERANCE = 0.85
DEFAULT_THROUGHPUT_MB_S = 75

# Cluster restarted: 08/07 
SERVICE_ACCOUNT_EMAIL = "cluster-minimal-76536cff2f28@kafka-k8s-trial.iam.gserviceaccount.com"

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
        self.soak_test_process = None

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
                # input("Setup complete. Press any key to run the configuration...")
                self.run_configuration(configuration)

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

        if self.soak_test_process:
            self.soak_test_process.stop()

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
        if configuration["teardown_broker_nodes"]:
          self.unprovision_node_pool(configuration)
        else:
          print("Broker nodes left standing.")

    # run a script to deploy kafka
    def k8s_deploy_kafka(self, num_partitions):
        print(f"Deploying Kafka with {num_partitions} partitions...")
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

    def get_zookeepers_count(self):
        filename = "./get-zookeepers-count.sh"
        args = [filename]

        zk_count = 0
        try:
            zk_count = int(self.bash_command_with_output(args, SCRIPT_DIR))
        except ValueError:
            pass

        print(f"zk={zk_count}")
        return zk_count

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

    def check_zookeepers(self, expected_zk_count):
        return self.get_zookeepers_count() == expected_zk_count

    def check_zookeepers_ok(self):
        i = 1
        attempts = 7
        check_zks = self.check_zookeepers(3)
        while not check_zks:
            time.sleep(20)

            check_zks = self.check_zookeepers(3)
            print(f"Waiting for zks to start...{i}/{attempts}")
            i += 1
            if i > attempts:
                print("Error: Time-out waiting for zks to start.")
                return False

        print("ZKs started ok.")
        return True

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
        # where num_partitions = max(#P, #C), where #P = TT / 75)
        # see https://docs.cloudera.com/runtime/7.1.0/kafka-performance-tuning/topics/kafka-tune-sizing-partition-number.html 
        num_partitions = configuration["number_of_brokers"] * 3 
        self.k8s_deploy_kafka(num_partitions)

        # Configure # kafka brokers
        self.k8s_scale_brokers(str(configuration["number_of_brokers"]))

        all_ok = self.check_brokers_ok(configuration)
        if not all_ok:
            print("Aborting configuration - brokers not ok.")
            return False

        # deploy producers/consumers
        self.k8s_deploy_producers_consumers()

        # deploy burrow
        self.k8s_deploy_burrow()

        # scale consumers
        self.k8s_scale_consumers(str(configuration["num_consumers"]))
        
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

    def run_stress_test(self, configuration):
        print(f"\r\n3. Running stress test.")
        self.consumer_throughput_process = CheckConsumerThroughputProcess(configuration, self.consumer_throughput_queue)
        self.producer_increment_process = ProducerIncrementProcess(configuration, self.consumer_throughput_process)

        self.consumer_throughput_process.start()
        self.producer_increment_process.start()

        # join so that we wait for thread to finish
        # i.e. on degradation event
        self.consumer_throughput_process.join()

        # degradation occurred, stop the increment thread
        self.producer_increment_process.stop()

        print(f"\r\n3. Stress test completed.")

    def run_soak_test(self, configuration):
        print(f"\r\n3. Running soak test.")

        # start the thread for soak test
        self.soak_test_process.start()

        # wait for thread to exit
        self.soak_test_process.join()

        print(f"\r\n3. Soak test completed.")

    def run_configuration(self, configuration):
        print(f"\r\n3. Running configuration: {configuration}")

        # Configure producers with required number of initial producers and their message size
        # Note - number of producers may be greater than 0
        self.k8s_configure_producers(str(configuration["start_producer_count"]), str(configuration["message_size_kb"]))

        # run stress test
        self.run_stress_test(configuration)

        # run soak test
        self.run_soak_test(configuration)

    def get_configuration_uid(self):
        return str(uuid.uuid4().hex.upper()[0:6])

    def load_configurations(self):
        print("Loading configurations.")

        configuration_template = {"number_of_brokers": 5, "message_size_kb": 750, "start_producer_count": 1,
                                  "max_producer_count": 16, "num_consumers": 1, "producer_increment_interval_sec": 0,
                                  "machine_size": "n1-highmem-4", "disk_size": 100,
                                  "producer_increment_interval_sec": 10, "machine_size": "n1-standard-8",
                                  "disk_size": 100, "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5,
                                  "ignore_throughput_threshold": False, "teardown_broker_nodes": False}

        d = {"configuration_uid": self.get_configuration_uid(), "start_producer_count": 9}
        self.configurations.append(dict(configuration_template, **d))
        d = {"configuration_uid": self.get_configuration_uid(), "num_consumers": 2, "start_producer_count": 9}
        self.configurations.append(dict(configuration_template, **d))
        d = {"configuration_uid": self.get_configuration_uid(), "num_consumers": 3, "start_producer_count": 9}
        self.configurations.append(dict(configuration_template, **d))
        d = {"configuration_uid": self.get_configuration_uid(), "num_consumers": 4, "start_producer_count": 4}
        self.configurations.append(dict(configuration_template, **d))
        d = {"configuration_uid": self.get_configuration_uid(), "num_consumers": 5, "max_producer_count": 14}
        self.configurations.append(dict(configuration_template, **d))
        # Final configuration should bring down the nodes
        d = {"configuration_uid": self.get_configuration_uid(), "num_consumers": 6, "max_producer_count": 12,
             "teardown_broker_nodes": True}
        self.configurations.append(dict(configuration_template, **d))

    def provision_node_pool(self, configuration):
        print(f"\r\n1. Provisioning node pool: {configuration}")

        filename = "./generate-kafka-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL, configuration["machine_size"], str(configuration["disk_type"]),
                str(configuration["disk_size"]), str(configuration["number_of_brokers"])]
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


class SoakTestThread(StoppableProcess):

    def __init__(self, configuration, queue):
        super().__init__()

        self.configuration = configuration
        self.consumer_throughput_queue = queue

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

        return producer_count

    def decrement_producer_count(self):
        producer_count = self.get_producer_count()
        print(f"Current producer count is {producer_count}")

        # decrement the producer count
        producer_count -= 1

        # decrement the producer count
        self.k8s_scale_producers(producer_count)
        print(f"Decrementing the producer count to {producer_count}")

    def run(self):
        num_brokers = self.configuration["num_brokers"]

        self.decrement_producer_count()

        # decrement the producers further until stability is achieved
        while not self.is_stopped():
            try:
                # check the queue
                job = self.consumer_throughput_queue.reserve(timeout=1)

                if job is None:
                    print("Nothing on consumer throughput queue...")
                    # sleep for 10ms
                    time.sleep(.10)
                    continue

                data = json.loads(job.body)

                # print(f"Received data {data} on consumer throughput queue.")

                consumer_id = data["consumer_id"]
                throughput = data["throughput"]
                num_producers = data["producer_count"]

                # append to specific list (as stored in dict)
                self.consumer_throughput_dict[consumer_id].append(throughput)

                if len(self.consumer_throughput_dict[consumer_id]) >= 3:
                    # truncate list to last 3 entries
                    self.consumer_throughput_dict[consumer_id] = self.consumer_throughput_dict[consumer_id][-3:]

                    consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id])
                    print(f"Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                    consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                    if consumer_throughput_average < consumer_throughput_tolerance:
                        print(f"Warning: Consumer {consumer_id} throughput average {consumer_throughput_average} is below tolerance {consumer_throughput_tolerance}")
                        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

                        # stop after 3 consecutive threshold events
                        if self.threshold_exceeded[consumer_id] >= 3:
                            print("Threshold (still exceeded) decrementing producer count")
                            self.decrement_producer_count()
                        else:
                            # reset the threshold events (since they must be consecutive to force an event)
                            self.threshold_exceeded[consumer_id] = 0

        num_producers = self.get_producer_count()
        print(f"Throughput stability achieved @ {num_producers} producers.")

        # start soak test once stability achieved
        soak_test_s = (313 * num_brokers) / num_producers
        print(f"Running soak test for {soak_test_s} seconds.")

        start_time = time.now()
        while not self.is_stopped() and ((time.now() - start_time) < soak_test_s):
            try:
                # check the queue
                job = self.consumer_throughput_queue.reserve(timeout=1)

                if job is None:
                    # print("Nothing on consumer throughput queue...")
                    # sleep for 10ms
                    time.sleep(.10)
                    continue

                data = json.loads(job.body)
                print(f"Received data {data} on consumer throughput queue.")

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

        print(f"Soak test completed after {soak_test_s} seconds.")


class CheckConsumerThroughputProcess(StoppableProcess):

    def __init__(self, configuration, queue):
        super().__init__()
        self.configuration = configuration
        self.consumer_throughput_queue = queue
        self.threshold_exceeded = {} 
        self.consumer_throughput_dict = defaultdict(list)
        self.lock = threading.Lock()

    def clear_consumer_throughput(self):
       with self.lock:
           for key in self.consumer_throughput_dict.keys():
               self.consumer_throughput_dict[key].clear()
       print("Flushed consumer throughput values.")

    def run(self):
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
                    # print("Nothing on consumer throughput queue...")
                    # sleep for 10ms
                    time.sleep(.10)
                    continue

                data = json.loads(job.body)

                # print(f"Received data {data} on consumer throughput queue.")

                consumer_id = data["consumer_id"]
                throughput = data["throughput"]
                num_producers = data["producer_count"]

                if not self.configuration["ignore_throughput_threshold"]:
                    with self.lock:
                        # append to specific list (as stored in dict)
                        self.consumer_throughput_dict[consumer_id].append(throughput)

                        if len(self.consumer_throughput_dict[consumer_id]) >= 10:
                            # truncate list to last 10 entries
                            self.consumer_throughput_dict[consumer_id] = self.consumer_throughput_dict[consumer_id][-10:]

                            consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id])
                            print(f"Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                            consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                            if consumer_throughput_average < consumer_throughput_tolerance:
                                print(f"Warning: Consumer {consumer_id} throughput average {consumer_throughput_average} is below tolerance {consumer_throughput_tolerance}")
                                self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

                                # stop after 3 consecutive threshold events
                                if self.threshold_exceeded[consumer_id] >= 3:
                                    print("Stopping after 3 consecutive threshold events...")
                                    self.stop()
                            else:
                                # reset the threshold events (since they must be consecutive to force an event)
                                self.threshold_exceeded[consumer_id] = 0


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

    def __init__(self, configuration, consumer_throughput_process):
        super().__init__()
        self.configuration = configuration
        self.consumer_throughput_process = consumer_throughput_process

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

        return producer_count

    def run(self):
        desired_producer_count = self.configuration["start_producer_count"] 
        while not self.is_stopped() and (desired_producer_count <= self.configuration["max_producer_count"]):
            i = 1
            producer_count = self.get_producer_count()
            attempts = (desired_producer_count - producer_count) * 40 
            while not self.is_stopped() and producer_count != desired_producer_count:
                time.sleep(5)
                producer_count = self.get_producer_count()
                print(f"(Still) waiting for producer(s) to start... ({i}/{attempts})")
                i += 1
                if i > attempts:
                    print("Error: Timeout waiting for producer(s) to start...")
                    self.stop()
                    break

            # clear the list of entries for all consumers
            self.consumer_throughput_process.clear_consumer_throughput()

            desired_producer_count += 1

            # Wait for a specified interval before starting the next producer
            producer_increment_interval_sec = self.configuration["producer_increment_interval_sec"]
            print(f"Waiting {producer_increment_interval_sec}s before starting next producer.")
            time.sleep(producer_increment_interval_sec)

            print(f"Starting next producer...")
            self.k8s_scale_producers(str(desired_producer_count))

# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    print("Reminder: have you remembered to update the SERVICE_ACCOUNT_EMAIL (if cluster has been bounced?)")
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = Controller(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
