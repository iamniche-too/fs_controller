import time
import unittest

from controller import Controller


class TestController(unittest.TestCase):
    controller = None

    def setUp(self):
        self.controller = Controller()

    def tearDown(self):
        pass

    def test_scale_brokers(self):
        self.controller.k8s_scale_brokers(3)
        time.sleep(20)
        broker_count = self.controller.get_broker_count()
        print(f"broker_count={broker_count}")
        self.assertEqual(3, broker_count)

    def test_scale_producers(self):
        self.controller.k8s_scale_producers(1)
        time.sleep(10)
        producer_count = self.controller.get_producer_count()
        print(f"producer_count={producer_count}")
        self.assertEqual(1, producer_count)

