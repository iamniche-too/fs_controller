import logging
from io import StringIO

import greenstalk

from fs.controller import Controller


class PartitionCountController(Controller):

    def get_configuration_description(self):
        return "Testing partition counts"

    def load_configurations(self):
        """
        Configurations pertaining to changing message size
        :return:
        """
        logging.info("Loading partition count configurations.")

        run_uid = self.get_run_uid()

        # override the partition count
        d = {"run_uid": run_uid, "number_of_partitions": 5}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the partition count
        d = {"run_uid": run_uid, "number_of_partitions": 15}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the partition count
        d = {"run_uid": run_uid, "number_of_partitions": 30}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = PartitionCountController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
