import greenstalk

from fs.controller import Controller


class MessageSizeController(Controller):

    def load_configurations(self):
        """
        Configurations pertaining to changing message size

        :return:
        """
        print("Loading message size configurations.")

        # override the message size
        d = {"message_size_kb": 75}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the message size
        d = {"message_size_kb": 750}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the message size
        d = {"message_size_kb": 7500}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    print("Reminder: have you remembered to update the SERVICE_ACCOUNT_EMAIL (if cluster has been bounced?)")
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = MessageSizeController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
