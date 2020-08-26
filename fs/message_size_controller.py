import types

import greenstalk

from fs.controller import Controller
from fs.soak_test_process import SoakTestProcess
from fs.stress_test_process import StressTestProcess
from fs.utils import addlogger


@addlogger
class MessageSizeController(Controller):

    def get_configuration_description(self):
        return "Testing message sizes"

    def load_configurations(self):
        """
        Configurations pertaining to changing message size

        :return:
        """
        self.__log.info("Loading message size configurations.")

        broker_count = self.configuration_template["number_of_brokers"]

        # override the message size
        d = {"run_uid": self.run_uid, "message_size_kb": 75, "start_producer_count": (broker_count*2)-1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template, broker_count))

        # override the message size
        d = {"run_uid": self.run_uid, "message_size_kb": 750, "start_producer_count": (broker_count*2)-1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template, broker_count))

        # override the message size
        d = {"run_uid": self.run_uid, "message_size_kb": 7500, "start_producer_count": (broker_count*2)-1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template, broker_count))

    def get_soak_test_process(self, configuration, queue):
        """
        override default

        :param configuration:
        :param queue:
        :return:
        """
        return SoakTestProcess2(configuration, queue)

    def get_stress_test_process(self, configuration, queue):
        """
        override default
        :param configuration:
        :param queue:
        :return:
        """
        return StressTestProcess2(configuration, queue)


class SoakTestProcess2(SoakTestProcess):
    def get_metrics_features(self):
        """
        Overridden implementation
        :return:
        """
        return ["num_consumers", "message_size_kb"]


class StressTestProcess2(StressTestProcess):
    def get_metrics_features(self):
        """
        Overridden implementation
        :return:
        """
        return ["num_consumers", "message_size_kb"]


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = MessageSizeController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
