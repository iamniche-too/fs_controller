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

        broker_count = self.configuration_template["number_of_brokers"]

        # override the replication factor
        d = {"run_uid": self.run_uid, "replication_factor": 1, "start_producer_count": (broker_count*2)-1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template), broker_count+1)

        # override the replication factor
        d = {"run_uid": self.run_uid, "replication_factor": 2, "start_producer_count": broker_count-1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template), broker_count+1)

        # override the replication factor
        d = {"run_uid": self.run_uid, "replication_factor": 3, "start_producer_count": 1}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template), broker_count+1)

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
        return ["num_consumers", "replication_factor"]


class StressTestProcess2(StressTestProcess):
    def get_metrics_features(self):
        """
        Return metrics features of interest
        :return:
        """
        return ["num_consumers", "replication_factor"]


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = ReplicationFactorController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
