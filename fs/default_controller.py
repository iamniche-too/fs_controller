import greenstalk

from fs.controller import Controller


class DefaultController(Controller):

    def load_configurations(self):
        """
        Default configurations

        :return:
        """
        print("Loading default configurations.")

        # no overrides on the default template
        self.configurations.append(self.get_configurations(self.configuration_template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    print("Reminder: have you remembered to update the SERVICE_ACCOUNT_EMAIL (if cluster has been bounced?)")
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = DefaultController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
