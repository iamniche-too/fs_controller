import subprocess
import time
import greenstalk
import threading
import requests
import json
import uuid
from statistics import mean
from collections import defaultdict

# Ranges between 22-50+
K8S_SERVICE_COUNT = 22

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "./scripts"
TERRAFORM_DIR = "./terraform/"

# set this to where-ever fs-kafka-k8s is cloned
#KAFKA_DEPLOY_DIR = "/home/nicholas/workspace/fs-kafka-k8s/"
KAFKA_DEPLOY_DIR = "/data/open-platform-checkouts/fs-kafka-k8s"
#PRODUCERS_CONSUMERS_DEPLOY_DIR = "/home/nicholas/workspace/fs-producer-consumer-k8s"
PRODUCERS_CONSUMERS_DEPLOY_DIR = "/data/open-platform-checkouts/fs-producer-consumer-k8s"

DEFAULT_CONSUMER_TOLERANCE = 0.9
DEFAULT_THROUGHPUT_MB_S = 75
PRODUCER_STARTUP_INTERVAL_S=26

# Cluster restarted: 29/04 @ 1041 
SERVICE_ACCOUNT_EMAIL = "cluster-minimal-53f4250eb059@kafka-k8s-trial.iam.gserviceaccount.com"

CLUSTER_NAME="gke-kafka-cluster"
CLUSTER_ZONE="europe-west2-a"

ENDPOINT_URL = "http://focussensors.duckdns.org:9000/consumer_reporting_endpoint"
stop_threads = False

class Controller:
    configurations = []

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

    def post_json(self, endpoint_url, payload):
      headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
      request = requests.post(endpoint_url, data=json.dumps(payload), headers=headers)

    def run(self):
        self.load_configurations()

        for configuration in self.configurations:
            print("\r\nTrying next configuration...")

            self.provision_node_pool(configuration)

            # only run if everything is ok
            if self.setup_configuration(configuration):
                #input(f"Setup complete. Press any key to run the configuration {configuration}")
                self.run_configuration(configuration)

            # now teardown and unprovision
            self.teardown_configuration(configuration)

    def k8s_delete_namespace(self, namespace):
        print(f"Deleting namespace: {namespace}")
        # run a script to delete a specific namespace
        filename = "./delete-namespace.sh"
        args = [filename, namespace]
        self.bash_command_with_output(args, SCRIPT_DIR)

    def flush_consumer_throughput_queue(self):
        # no flush() method exists in greenstalk so need to do it "brute force"
        print("Flushing consumer throughput queue")

        # stats = self.consumer_throughput_queue.stats_tube("consumer_throughput")
        # print(stats)

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

    def teardown_configuration(self, configuration):
        print(f"\r\n4. Teardown configuration: {configuration}")

        global stop_threads

        # Remove producers & consumers
        self.k8s_delete_namespace(PRODUCER_CONSUMER_NAMESPACE)

        # Remove kafka brokers
        self.k8s_delete_namespace(KAFKA_NAMESPACE)

        # stop reading the consumer queue
        stop_threads = True

        # wait for thread to exit
        time.sleep(5)

        # flush the consumer throughput queue
        self.flush_consumer_throughput_queue()

        # take down the kafka node pool
        self.unprovision_node_pool(configuration)

    # run a script to deploy kafka
    def k8s_deploy_kafka(self, num_partitions):
        print(f"k8s_deploy_kafka")
        filename = "./deploy.sh"
        args = [filename, str(num_partitions)]
        self.bash_command_with_wait(args, KAFKA_DEPLOY_DIR)

    # run a script to deploy kafka
    def k8s_deploy_producers_consumers(self):
        print(f"k8s_deploy_producers_consumers")
        filename = "./deploy/gcp/deploy.sh"
        args = [filename]
        self.bash_command_with_wait(args, PRODUCERS_CONSUMERS_DEPLOY_DIR)

    def k8s_scale_brokers(self, broker_count):
        print(f"k8s_scale_brokers, broker_count={broker_count}")
        # run a script to start brokers
        filename = "./scale-brokers.sh"
        args = [filename, str(broker_count)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def k8s_configure_producers(self, start_producer_count, message_size):
        print(f"k8s_configure_producers, start_producer_count={start_producer_count}, message_size={message_size}")
        filename = "./configure-producers.sh"
        args = [filename, str(start_producer_count), str(message_size)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def k8s_scale_consumers(self, num_consumers):
        print(f"k8s_configure_consumers, num_consumers={num_consumers}")
        filename = "./scale-consumers.sh"
        args = [filename, str(num_consumers)]
        self.bash_command_with_wait(args, SCRIPT_DIR)

    def check_k8s(self):
        print(f"check_k8s")
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

    def check_producers(self, expected_producer_count):
        actual_producer_count = self.get_producer_count()
        print(f"actual_producer_count={actual_producer_count}, expected_producer_count={expected_producer_count}")
        if actual_producer_count == expected_producer_count:
          return True
        else:
          return False

    def check_brokers_ok(self, configuration):
        i = 1
        attempts = configuration["number_of_brokers"]*5
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

        # deploy producers/consumers
        self.k8s_deploy_producers_consumers()

        # Configure # kafka brokers
        self.k8s_scale_brokers(str(configuration["number_of_brokers"]))

        all_ok = self.check_brokers_ok(configuration)
        if not all_ok:
            print("Aborting configuration - brokers not ok.")
            return False

        # scale consumers
        self.k8s_scale_consumers(str(configuration["num_consumers"]))

        # Configure producers with required message size
        self.k8s_configure_producers(str(configuration["start_producer_count"]), str(configuration["message_size_kb"]))

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

    def increment_producers_thread(self, configuration):
        actual_producer_count = 1
        while actual_producer_count <= configuration["max_producer_count"]:
            print(f"Starting producer {actual_producer_count}")
            # Start a new producer
            self.k8s_scale_producers(str(actual_producer_count))

            time.sleep(PRODUCER_STARTUP_INTERVAL_S)

            i = 1 
            check_producers = self.check_producers(actual_producer_count)
            while not check_producers:
                time.sleep(5)
                check_producers = self.check_producers(actual_producer_count)
                print(f"(Still) waiting for producer to start... ({i}/24)")
                i += 1
                if i > 24:
                    print("Error: Timeout waiting for producer to start...")
                    exit()

            # Wait for a specified interval before starting the next producer
            producer_increment_interval_sec = configuration["producer_increment_interval_sec"]
            print(f"Waiting {producer_increment_interval_sec}s before starting next producer.")
            time.sleep(producer_increment_interval_sec)

            actual_producer_count += 1

    def check_consumer_throughput(self, configuration):
        # create a dictionary of lists
        consumer_throughput_dict = defaultdict(list)

        done = False
        global stop_threads

        while done is False and stop_threads is False:
            try:
                job = self.consumer_throughput_queue.reserve(timeout=5)
                data = json.loads(job.body)

                print(f"Received data {data} on consumer throughput queue.")

                consumer_id = data["consumer_id"]
                throughput = data["throughput"]
                num_producers = data["producer_count"]

                # append to specific list (as stored in dict)
                consumer_throughput_dict[consumer_id].append(throughput)

                if len(consumer_throughput_dict[consumer_id]) >= 10:
                    # truncate list to last 10 entries
                    consumer_throughput_dict[consumer_id] = consumer_throughput_dict[consumer_id][-10:]

                    consumer_throughput_average = mean(consumer_throughput_dict[consumer_id])
                    print(f"Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                    consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)
                    if consumer_throughput_average < consumer_throughput_tolerance:
                        print(f"Warning: Consumer {consumer_id} throughput average {consumer_throughput_average} is below tolerance {consumer_throughput_tolerance}, exiting...")
                        done = True

                # Finally delete from queue
                self.consumer_throughput_queue.delete(job)

            except greenstalk.TimedOutError:
                print("Warning: nothing in consumer throughput queue.")
            except greenstalk.UnknownResponseError:
                print("Warning: unknown response from beanstalkd server.")
            except greenstalk.ConnectionError as ce:
                print(f"Error: ConnectionError: {ce}")

            time.sleep(int(configuration["consumer_throughput_reporting_interval"]))

    def run_configuration(self, configuration):
        print(f"\r\n3. Running configuration: {configuration}")

        thread1 = threading.Thread(target=self.increment_producers_thread, args=(configuration,))
        thread1.start()
        time.sleep(5)

        thread2 = threading.Thread(target=self.check_consumer_throughput, args=(configuration,))
        thread2.start()

        thread1.join()
        # no need to wait for second thread to finish
        # thread2.join()

        print(f"Run completed for configuration {configuration}")

    def load_configurations(self):
        print("Loading configurations.")

        configuration_uid = str(uuid.uuid4().hex.upper()[0:6])

        #configuration_3_750_n1_standard_1 = {
        configuration_template = {
                "configuration_uid": configuration_uid,
                "number_of_brokers": 3, "message_size_kb": 750, "start_producer_count": 3, "max_producer_count": 9, "num_consumers": 3,
                "producer_increment_interval_sec": 180, "machine_size": "n1-highmem-2", "disk_size": 100,
                "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5}

        self.configurations.append(dict(configuration_template))
        #self.configurations.append(dict(configuration_template, message_size_kb=7500))

    def provision_node_pool(self, configuration):
        print(f"\r\n1. Provisioning node pool: {configuration}")

        filename = "./generate-kafka-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL, configuration["machine_size"], str(configuration["disk_type"]), str(configuration["disk_size"])]
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

# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    print("Reminder: have you remembered to update the SERVICE_ACCOUNT_EMAIL (if cluster has been bounced?)")
    c = Controller()
    c.run()
