import time
import unittest

from controller import Controller

# IMPORTANT: Please set the directory to be the root directory NOT /tests/
class TestController(unittest.TestCase):
    controller = None

    def setUp(self):
        self.controller = Controller()

    def tearDown(self):
        pass

    # IMPORTANT: set ENV var GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/kafka-k8s-trial-4287e941a38f.json
    def test_check_k8s(self):
        is_ok = self.controller.check_k8s()
        self.assertEqual(True, is_ok)

    # IMPORTANT: set ENV var GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/kafka-k8s-trial-4287e941a38f.json
    def test_scale_brokers(self):
        self.controller.k8s_scale_brokers(3)
        time.sleep(20)
        broker_count = self.controller.get_broker_count()
        print(f"broker_count={broker_count}")
        self.assertEqual(3, broker_count)

    # IMPORTANT: set ENV var GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/kafka-k8s-trial-4287e941a38f.json
    def test_scale_producers(self):
        self.controller.k8s_scale_producers(1)
        time.sleep(10)
        producer_count = self.controller.get_producer_count()
        print(f"producer_count={producer_count}")
        self.assertEqual(1, producer_count)

    def test_provision_node_pool(self):
        configuration = {"number_of_brokers": 3, "message_size_kb": 750, "max_producers": 3,
                       "producer_increment_interval_sec": 30, "machine_size": "n1-standard-1", "disk_size": 100,
                       "disk_type": "pd-standard"}
        self.controller.provision_node_pool(configuration)
        self.controller.unprovision_node_pool()
