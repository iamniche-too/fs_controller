import greenstalk

from fs.controller import Controller
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
        run_uid = self.get_run_uid()

        # override the message size
        d = {"run_uid": run_uid, "message_size_kb": 75}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

        # override the message size
        d = {"run_uid": run_uid, "message_size_kb": 750}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))

        # override the message size
        d = {"run_uid": run_uid, "message_size_kb": 7500}
        template = dict(self.configuration_template, **d)
        self.configurations.extend(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = MessageSizeController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
