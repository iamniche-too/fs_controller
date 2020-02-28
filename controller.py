import time

class Controller:
    configurations = []

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
        print("Run completed.")


    def load_configurations(self):
        print("Loading configurations.")
        # TODO - load from file?
        configuration_0 = {"number_of_brokers": 3, "message_size_kb": 750, "max_producers": 3, "producer_increment_interval_sec": 10}
        self.configurations.append(configuration_0)


if __name__ == '__main__':
    c = Controller()
    c.run()
