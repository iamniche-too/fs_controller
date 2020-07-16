import greenstalk

from fs.controller import Controller


class ReplicationFactorController(Controller):

    def get_configuration_description(self):
        return "Testing replication factor"

    def load_configurations(self):
        """
        Configurations pertaining to changing message size
        :return:
        """
        print("Loading replication factor configurations.")

        # override the replication factor
        d = {"replication_factor": 1}
        template = dict(self.configuration_template, **d)
        self.configurations.append(self.get_configurations(template))

        # override the replication factor
        d = {"replication_factor": 3}
        template = dict(self.configuration_template, **d)

        # override the replication factor
        d = {"replication_factor": 5}
        template = dict(self.configuration_template, **d)

        self.configurations.append(self.get_configurations(template))


# GOOGLE_APPLICATION_CREDENTIALS=./kafka-k8s-trial-4287e941a38f.json
if __name__ == '__main__':
    print("Reminder: have you remembered to update the SERVICE_ACCOUNT_EMAIL (if cluster has been bounced?)")
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')
    c = ReplicationFactorController(consumer_throughput_queue)
    c.flush_consumer_throughput_queue()
    c.run()
