import greenstalk

from fs.controller import Controller
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

        run_uid = self.get_run_uid()

        # override the replication factor
        d = {"run_uid": run_uid, "replication_factor": 1}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the replication factor
        d = {"run_uid": run_uid, "replication_factor": 3}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the replication factor
        d = {"run_uid": run_uid, "replication_factor": 5}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = ReplicationFactorController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
