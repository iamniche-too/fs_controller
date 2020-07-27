import unittest

import greenstalk

from fs.message_size_controller import MessageSizeController


class TestMessageSizeController(unittest.TestCase):
    def setUp(self):
        self.queue = greenstalk.Client(host='127.0.0.1', port=12000, use='consumer_throughput',
                                       watch='consumer_throughput')
        c = MessageSizeController(self.queue)

    def tearDown(self):
        self.queue.close()

    def test_x(self):
        c.get_metrics_features()
