import unittest

from controller import Controller


class IntegrationTestK8SCount(unittest.TestCase):
    controller = None

    def setUp(self):
        self.controller = Controller()

    def tearDown(self):
        pass

    def test_get_producer_count(self):
        controller = Controller()
        producer_count = self.controller.get_producer_count()
        self.assertEqual(0, producer_count)

    def test_get_broker_count(self):
        controller = Controller()
        broker_count = controller.get_producer_count()
        self.assertEqual(3, broker_count)
