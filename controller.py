import subprocess
import time
import greenstalk
import threading
from statistics import mean

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "./scripts"
TERRAFORM_DIR = "./terraform/"

DEFAULT_CONSUMER_TOLERANCE = 0.9
DEFAULT_THROUGHPUT_MB_S = 75

SERVICE_ACCOUNT_EMAIL = "cluster-minimal-4752c47e1515@kafka-k8s-trial.iam.gserviceaccount.com"

class Controller:
    configurations = []

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

    def run(self):
        self.load_configurations()

        for configuration in self.configurations:
            self.provision_node_pool(configuration)

            # only run if everything is ok
            if self.setup_configuration(configuration):
                self.run_configuration(configuration)

        # now teardown and unprovision
        self.teardown_configuration(configuration)

    def k8s_delete_namespace(self, namespace):
        print(f"Deleting namespace: {namespace}")
        # run a script to delete a specific namespace
        filename = "./delete-namespace.sh"
        args = [str(directory + filename), namespace]
        self.bash_command_with_output(args, SCRIPT_DIR)

    def flush_consumer_throughput_queue(self):
        # no flush() method exists in greenstalk so need to do it "brute force"
        print("Flushing consumer throughput queue")

        stats = self.consumer_throughput_queue.stats_tube("consumer_throughput")
        print(stats)

        done = False
        while not done:
            job = self.consumer_throughput_queue.reserve(timeout=0)
            if job is None:
                done = True
            self.consumer_throughput_queue.delete(job)

        print("Consumer throughput queue flushed.")

    def teardown_configuration(self, configuration):
        print(f"4. Teardown configuration: {configuration}")

        # Remove producers & consumers
        self.k8s_delete_namespace(PRODUCER_CONSUMER_NAMESPACE)

        # Remove kafka brokers
        self.k8s_delete_namespace(KAFKA_NAMESPACE)

        # flush the consumer throughput queue
        self.flush_consumer_throughput_queue()

        # take down the kafka node pool
        self.unprovision_node_pool(configuration)

    def k8s_scale_brokers(self, broker_count):
        print(f"k8s_scale_brokers, broker_count={broker_count}")
        # run a script to start brokers
        filename = "./scale-brokers.sh"
        args = [filename, str(broker_count)]
        self.bash_command_no_output(args, SCRIPT_DIR)

    def k8s_configure_producers(self, message_size):
        print(f"k8s_configure_producers, message_size={message_size}")
        filename = "./configure-producers.sh"
        args = [filename, str(message_size)]
        self.bash_command_no_output(args, SCRIPT_DIR)

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
        return self.get_producer_count() == expected_producer_count

    def check_brokers_ok(self, configuration):
        print("Waiting for brokers to start...")
        i = 0
        check_brokers = self.check_brokers(configuration["number_of_brokers"])
        while not check_brokers:
            time.sleep(20)

            check_brokers = self.check_brokers(configuration["number_of_brokers"])
            print("(Still) waiting for brokers to start...")
            i += 1
            if i > 6:
                print("Error: Timeout waiting for brokers to start.")
                return False

        print("Brokers started ok.")
        return True

    def setup_configuration(self, configuration):
        print(f"2. Setup configuration: {configuration}")

        # Configure # kafka brokers
        self.k8s_scale_brokers(str(configuration["number_of_brokers"]))

        all_ok = self.check_brokers_ok(configuration)

        if all_ok:
            # Configure producers with required message size
            self.k8s_configure_producers(str(configuration["message_size_kb"]))

        return all_ok

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, cwd=working_directory)
        out = p.communicate()[0].decode("UTF-8")
        return out

    def bash_command_with_wait(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        try:
            subprocess.check_call(args, stderr=subprocess.STDOUT, cwd=working_directory)
        except subprocess.CalledProcessError:
            # There was an error - command exited with non-zero code
            exit()

    def bash_command_no_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        subprocess.Popen(args, cwd=working_directory)

    def k8s_scale_producers(self, producer_count):
        filename = "./scale-producers.sh"
        args = [filename, str(producer_count), SCRIPT_DIR]
        self.bash_command_with_output(args)

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
        while actual_producer_count <= configuration["max_producers"]:
            print(f"Starting producer {actual_producer_count}")
            # Start a new producer
            self.k8s_scale_producers(str(actual_producer_count))

            time.sleep(10)

            i = 0
            check_producers = self.check_producers(actual_producer_count)
            while not check_producers:
                time.sleep(1)
                check_brokers = self.check_producers(actual_producer_count)
                print("(Still) waiting for producer to start...")
                i += 1
                if i > 3:
                    print("Error: Timeout waiting for producer to start...")
                    exit()

            # Wait for a specified interval before starting the next producer
            producer_increment_interval_sec = configuration["producer_increment_interval_sec"]
            print(f"Waiting {producer_increment_interval_sec}s before starting next producer.")
            time.sleep(producer_increment_interval_sec)

            actual_producer_count += 1

    def check_consumer_throughput(self, configuration):
        throughput_list = []

        done = False

        while done is False:
            print(f"Checking consumer throughput queue...")

            try:
                job = self.consumer_throughput_queue.reserve(timeout=5)
                consumer_throughput = float(job.body)

                # append to the list
                throughput_list.append(consumer_throughput)

                if len(throughput_list) >= 3:
                    # truncate list to last 3 entries
                    throughput_list = throughput_list[-3:]

                    consumer_throughput_average = mean(throughput_list)
                    print(f"Consumer throughput (average) = {consumer_throughput_average}")

                    consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * (configuration["consumer_tolerance"] or DEFAULT_CONSUMER_TOLERANCE))
                    if consumer_throughput_average < consumer_throughput_tolerance:
                        print(f"Consumer throughput average {consumer_throughput_average} is below tolerance {consumer_throughput_tolerance})")

                    done = True

                # Finally delete from queue
                self.consumer_throughput_queue.delete(job)

            except greenstalk.TimedOutError:
                print("Warning: nothing in consumer throughput queue.")

            time.sleep(int(configuration["consumer_throughput_reporting_interval"] or 5))

    def run_configuration(self, configuration):
        print(f"3. Running configuration: {configuration}")

        thread1 = threading.Thread(target=self.increment_producers_thread, args=(configuration,))
        thread2 = threading.Thread(target=self.check_consumer_throughput, args=(configuration,))

        thread2.start()
        thread1.start()
        thread1.join()
        # no need to wait for second thread to finish
        # thread2.join()

        print(f"Run completed for configuration {configuration}")

    def load_configurations(self):
        print("Loading configurations.")

        configuration_3_750_n1_standard_1 = {"number_of_brokers": 3, "message_size_kb": 750, "max_producers": 3,
                           "producer_increment_interval_sec": 30, "machine_size": "n1-standard-1", "disk_size": 100, "disk_type": "pd-standard"}

        # configuration_5_750_n1_standard_1 = {"number_of_brokers": 5, "message_size_kb": 750, "max_producers": 3,
        #                   "producer_increment_interval_sec": 30, "machine_size": "n1-standard-1", "disk_size": 100,
        #                   "disk_type": "pd-standard"}

        self.configurations.append(configuration_3_750_n1_standard_1)
        self.configurations.append(configuration_3_750_n1_standard_1)

    def provision_node_pool(self, configuration):
        print(f"1. Provisioning node pool: {configuration}")

        filename = "./generate-kafka-node-pool.sh"
        args = [filename, SERVICE_ACCOUNT_EMAIL, configuration["machine_size"], str(configuration["disk_type"]), str(configuration["disk_size"])]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        filename = "./provision.sh"
        args = [filename]
        self.bash_command_with_wait(args, TERRAFORM_DIR)

        print("Node pool provisioned.")

    def unprovision_node_pool(self, configuration):
        print(f"5. Unprovisioning node pool: {configuration}")
        filename = "unprovision.sh"
        args = [filename]
        self.bash_command_with_wait(args, TERRAFORM_DIR)
        print("Node pool unprovisioned.")

# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    c = Controller()
    c.run()
