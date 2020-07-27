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

    def get_metrics_features(self):
        """
        Return metrics features of interest
        :return:
        """
        return ["num_consumers", "number_of_partitions"]

    def get_soak_test_process(self, configuration, queue):
        """
        override default

        :param configuration:
        :param queue:
        :return:
        """
        soak_test_process = SoakTestProcess(configuration, queue)
        soak_test_process.get_metrics_features = types.MethodType(self.get_metrics_features, soak_test_process)
        return soak_test_process

    def get_stress_test_process(self, configuration, queue):
        """
        override default
        :param configuration:
        :param queue:
        :return:
        """
        stress_test_process = StressTestProcess(configuration, queue)
        stress_test_process.get_metrics_features = types.MethodType(self.get_metrics_features, stress_test_process)
        return stress_test_process


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = PartitionCountController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
