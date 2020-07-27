import types

import greenstalk

from fs.controller import Controller
from fs.soak_test_process import SoakTestProcess
from fs.stress_test_process import StressTestProcess
from fs.utils import addlogger


@addlogger
class PartitionCountController(Controller):

    def get_configuration_description(self):
        return "Testing partition counts"

    def load_configurations(self):
        """
        Configurations pertaining to changing partition count
        :return:
        """
        self.__log.info("Loading partition count configurations...")

        # override the partition count
        d = {"run_uid": self.run_uid, "number_of_partitions": 5}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

        # override the partition count
        d = {"run_uid": self.run_uid, "number_of_partitions": 15}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

        # override the partition count
        d = {"run_uid": self.run_uid, "number_of_partitions": 30}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

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
        Return metrics features of interest
        :return:
        """
        return ["num_consumers", "number_of_partitions"]


class StressTestProcess2(StressTestProcess):
    def get_metrics_features(self):
        """
        Return metrics features of interest
        :return:
        """
        return ["num_consumers", "number_of_partitions"]

# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = PartitionCountController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
