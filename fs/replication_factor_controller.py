import types

import greenstalk

from fs.controller import Controller
from fs.soak_test_process import SoakTestProcess
from fs.stress_test_process import StressTestProcess
from fs.utils import addlogger


@addlogger
class ReplicationFactorController(Controller):

    def get_configuration_description(self):
        return "Testing replication factor"

    def load_configurations(self):
        """
        Configurations pertaining to changing replication factor
        :return:
        """
        self.__log.info("Loading replication factor configurations.")

        # override the replication factor
        d = {"run_uid": self.run_uid, "replication_factor": 1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

        # override the replication factor
        d = {"run_uid": self.run_uid, "replication_factor": 3}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

        # override the replication factor
        d = {"run_uid": self.run_uid, "replication_factor": 5}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

    def get_metrics_features(self):
        """
        Return metrics features of interest
        :return:
        """
        return ["num_consumers", "replication_factor"]

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
    c = ReplicationFactorController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
