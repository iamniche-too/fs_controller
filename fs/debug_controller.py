import greenstalk

from fs.controller import Controller
from fs.utils import addlogger


@addlogger
class DebugController(Controller):
    """
    Debug controller with hooks to prompt for input to continue after each stage
    """
    def post_broker_timeout_hook(self):
        # input("Press any key to continue...")
        pass

    def post_stress_test_hook(self):
        # input("Press any key to continue...")
        pass

    def post_soak_test_hook(self):
        # input("Press any key to continue...")
        pass

    def post_setup_hook(self):
        """
        After setup, wait for key inout to continue
        i.e. we can pause with Kafka brokers running at this point
        :return:
        """
        # input("Press any key to continue...")
        pass

    def get_configuration_description(self):
        return "Default test"

    def load_configurations(self):
        """
        Default configurations

        :return:
        """
        self.__log.info("Loading default configurations.")

        broker_count = self.configuration_template["number_of_brokers"]
        
        d = {"configuration_uid": self.get_uid(), "description": self.get_configuration_description(), "run_uid": self.run_uid, "start_producer_count": 16, "num_consumers": 3}
        template = dict(self.configuration_template, **d)

        configurations = [template]
        self.configurations.extend(configurations)


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = DebugController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
