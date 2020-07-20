import greenstalk

from fs.controller import Controller
from fs.utils import addlogger


@addlogger
class DefaultController(Controller):

    def get_configuration_description(self):
        return "Default test"

    def load_configurations(self):
        """
        Default configurations

        :return:
        """
        self.__log.info("Loading default configurations.")

        d = {"run_uid": self.run_uid}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # run same test twice...
        d = {"run_uid": self.run_uid}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = DefaultController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
