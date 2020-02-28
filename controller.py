import time
import greenstalk

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

    def teardown_configuration(self, configuration):
        print(f"Teardown configuration: {configuration}")
        # Remove producers & consumers
        # Remove kafka brokers

    def setup_configuration(self, configuration):
        print(f"Setup configuration: {configuration}")
        # Ensure cluster is provisioned
        # Configure kafka brokers
        # Configure producer with message size

    def run_configuration(self, configuration):
        print(f"Running configuration: {configuration}")
        producer_count = 0
        while producer_count < configuration["max_producers"]:
            print(f"Starting producer {producer_count}")
            # Start a new producer
            producer_count += 1
            # Wait for a specified time
            producer_increment_interval_sec = configuration["producer_increment_interval_sec"]
            print(f"Waiting {producer_increment_interval_sec} seconds.")
            time.sleep(producer_increment_interval_sec)
            print("Reading producer_count queue...")
            job = self.producer_count_queue.reserve()
            if job:
                producer_count = int(job.body)
                print(f"Producer count={producer_count}")
                # now remove from the queue
                self.producer_count_queue.delete(job)

        print("Run completed.")


    def load_configurations(self):
        print("Loading configurations.")
        # TODO - load from file?
        configuration_0 = {"number_of_brokers": 3, "message_size_kb": 750, "max_producers": 3, "producer_increment_interval_sec": 1}

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
