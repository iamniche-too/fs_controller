import greenstalk

from fs.controller import Controller
from fs.utils import addlogger


@addlogger
class DefaultController(Controller):

    def post_broker_timeout_hook(self):
        input("Something went wrong with the broker deployment. Press any key to continue...")

    def post_setup_hook(self):
        """
        After setup, wait for key inout to continue
        i.e. we can pause with Kafka brokers running at this point
        :return:
        """
        input("Setup complete. Press any key to run the configuration...")

    def get_configuration_description(self):
        return "Default test"

    def load_configurations(self):
        """
        Default configurations

        :return:
        """
        self.__log.info("Loading default configurations.")

        broker_count = self.configuration_template["number_of_brokers"]
        
        d = {"run_uid": self.run_uid, "start_producer_count": (broker_count*2)-1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = DefaultController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
