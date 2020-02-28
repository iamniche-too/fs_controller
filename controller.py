import subprocess
import time
import greenstalk
import kubernetes

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "./scripts/"


class Controller:
    configurations = []

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    producer_count_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='producer_count')

    def run(self):
        self.load_configurations()

        for configuration in self.configurations:
            self.setup_configuration(configuration)
            self.run_configuration(configuration)
            self.teardown_configuration(configuration)

    def k8s_delete_namespace(self, namespace):
        # run a script to delete a specific namespace
        directory = SCRIPT_DIR
        filename = "delete-namespace.sh"
        args = [str(directory + filename), namespace]
        self.bash_command(args)

    def teardown_configuration(self, configuration):
        print(f"Teardown configuration: {configuration}")

        # Remove producers & consumers
        self.k8s_delete_namespace(PRODUCER_CONSUMER_NAMESPACE)

        # Remove kafka brokers
        self.k8s_delete_namespace(KAFKA_NAMESPACE)

    def k8s_start_brokers(self, broker_count):
        print(f"k8s_start_brokers, broker_count={broker_count}")

        # run a script to start brokers
        directory = SCRIPT_DIR
        filename = "start-brokers.sh"
        args = [str(directory + filename), broker_count]
        self.bash_command(args)

    def k8s_start_producers(self, message_size):
        print(f"k8s_start_producers, message_size={message_size}")

        # run a script to delete a specific namespace
        directory = SCRIPT_DIR
        filename = "start-producers.sh"
        args = [str(directory + filename), message_size]
        self.bash_command(args)

    def setup_configuration(self, configuration):
        print(f"Setup configuration: {configuration}")

        # Configure kafka brokers
        self.k8s_start_brokers(str(configuration["number_of_brokers"]))

        # Start 0 producers with message size
        self.k8s_start_producers(str(configuration["message_size_kb"]))

    def bash_command(self, additional_args):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        subprocess.Popen(args)

    def k8s_start_producer(self, producer_count):
        # run a script to delete a specific namespace
        directory = "./scripts/"
        filename = "patch-increment-producer.sh"
        args = [str(directory + filename), producer_count]
        self.bash_command(args)

    def run_configuration(self, configuration):
        print(f"Running configuration: {configuration}")
        producer_count = 0
        while producer_count < configuration["max_producers"]:
            print(f"Starting producer {producer_count}")
            # Start a new producer
            self.k8s_start_producer(str(producer_count))
            producer_count += 1
            time.sleep(5)

            print("Reading producer_count queue...")
            job = self.producer_count_queue.reserve()
            if job:
                 producer_count = int(job.body)
                 print(f"Producer count={producer_count}")
                 # now remove from the queue
                 self.producer_count_queue.delete(job)

            # Wait for a specific time
            producer_increment_interval_sec = configuration["producer_increment_interval_sec"]
            print(f"Waiting {producer_increment_interval_sec} seconds.")
            time.sleep(producer_increment_interval_sec)

        print("Run completed.")

    def load_configurations(self):
        print("Loading configurations.")
        # TODO - load from file?
        configuration_0 = {"number_of_brokers": 3, "message_size_kb": 750, "max_producers": 3,
                           "producer_increment_interval_sec": 1}

        # configuration_1 = {"number_of_brokers": 5, "message_size_kb": 750, "max_producers": 3,
        #                   "producer_increment_interval_sec": 2}
        # configuration_2 = {"number_of_brokers": 7, "message_size_kb": 750, "max_producers": 5,
        #                  "producer_increment_interval_sec": 1}

        self.configurations.append(configuration_0)
        # self.configurations.append(configuration_1)
        # self.configurations.append(configuration_2)


if __name__ == '__main__':
    c = Controller()
    c.run()
