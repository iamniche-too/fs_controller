import subprocess
import time
import greenstalk

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "./scripts/"


class Controller:
    configurations = []

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

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
        self.bash_command_no_output(args)

    def teardown_configuration(self, configuration):
        print(f"Teardown configuration: {configuration}")

        # Remove producers & consumers
        self.k8s_delete_namespace(PRODUCER_CONSUMER_NAMESPACE)

        # Remove kafka brokers
        self.k8s_delete_namespace(KAFKA_NAMESPACE)

    def k8s_scale_brokers(self, broker_count):
        print(f"k8s_scale_brokers, broker_count={broker_count}")
        # run a script to start brokers
        directory = SCRIPT_DIR
        filename = "scale-brokers.sh"
        args = [str(directory + filename), str(broker_count)]
        self.bash_command_no_output(args)

    def k8s_configure_producers(self, message_size):
        print(f"k8s_configure_producers, message_size={message_size}")
        directory = SCRIPT_DIR
        filename = "configure-producers.sh"
        args = [str(directory + filename), str(message_size)]
        self.bash_command_no_output(args)

    def get_broker_count(self):
        directory = SCRIPT_DIR
        filename = "get-brokers-count.sh"
        args = [str(directory + filename)]

        broker_count = 0
        try:
            broker_count = int(self.bash_command_with_output(args))
        except ValueError:
            pass

        print(f"broker_count={broker_count}")
        return broker_count

    def check_brokers(self, expected_broker_count):
        return self.get_broker_count() == expected_broker_count

    def check_producers(self, expected_producer_count):
        return self.get_producer_count() == expected_producer_count

    def setup_configuration(self, configuration):
        print(f"Setup configuration: {configuration}")

        # Configure # kafka brokers
        self.k8s_scale_brokers(str(configuration["number_of_brokers"]))

        print("Waiting for brokers to start...")
        i = 0
        check_brokers = self.check_brokers(configuration["number_of_brokers"])
        while not check_brokers:
            time.sleep(20)
            check_brokers = self.check_brokers(configuration["number_of_brokers"])
            print("(Still) waiting for brokers to start...")
            i += 1
            if i > 6:
                print("Error: unexpected # of brokers running...")
                exit()

        print("Brokers started ok.")

        # wait for things to settle...
        time.sleep(10)

        # Configure producers with required message size
        self.k8s_configure_producers(str(configuration["message_size_kb"]))

    def bash_command_with_output(self, additional_args):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        out = p.communicate()[0].decode("UTF-8")
        return out

    def bash_command_no_output(self, additional_args):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        subprocess.Popen(args)

    def k8s_scale_producers(self, producer_count):
        directory = "./scripts/"
        filename = "scale-producers.sh"
        args = [str(directory + filename), str(producer_count)]
        self.bash_command_no_output(args)

    def get_producer_count(self):
        directory = "./scripts/"
        filename = "get-producers-count.sh"
        args = [str(directory + filename)]

        producer_count = 0
        try:
            producer_count = int(self.bash_command_with_output(args))
        except ValueError:
            pass

        print(f"reported_producer_count={producer_count}")
        return producer_count

    def run_configuration(self, configuration):
        print(f"Running configuration: {configuration}")
        desired_producer_count = 1
        while desired_producer_count <= configuration["max_producers"]:
            print(f"Starting producer {desired_producer_count}")
            # Start a new producer
            self.k8s_scale_producers(str(desired_producer_count))

            time.sleep(10)

            i = 0
            check_producers = self.check_producers(desired_producer_count)
            while not check_producers:
                time.sleep(10)
                check_brokers = self.check_producers(desired_producer_count)
                print("(Still) waiting for producers to start...")
                i += 1
                if i > 3:
                    print("Error: unexpected # of producers running...")
                    exit()

            # Wait for a specific time
            producer_increment_interval_sec = configuration["producer_increment_interval_sec"]
            print(f"Waiting for {producer_increment_interval_sec} seconds.")
            time.sleep(producer_increment_interval_sec)
            desired_producer_count += 1

        print("Run completed.")

    def load_configurations(self):
        print("Loading configurations.")
        # TODO - load from file?
        configuration_0 = {"number_of_brokers": 3, "message_size_kb": 750, "max_producers": 3,
                           "producer_increment_interval_sec": 30}

        # configuration_1 = {"number_of_brokers": 5, "message_size_kb": 750, "max_producers": 3,
        #                   "producer_increment_interval_sec": 20}
        # configuration_2 = {"number_of_brokers": 7, "message_size_kb": 750, "max_producers": 5,
        #                  "producer_increment_interval_sec": 10}

        self.configurations.append(configuration_0)
        # self.configurations.append(configuration_1)
        # self.configurations.append(configuration_2)


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    c = Controller()
    c.run()
