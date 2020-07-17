import time
from statistics import mean
from collections import defaultdict

from fs.base_process import BaseProcess
from fs.throughput_process import ThroughputProcess
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE


class CheckConsumerThroughputProcess(ThroughputProcess):
    """
    Check consumer throughput process
    """
    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        """
        Throughput below tolerance

        :param consumer_id:
        :return:
        """
        super().throughput_tolerance_exceeded(consumer_id, consumer_throughput_average, consumer_throughput_tolerance)

        # stop after 3 consecutive threshold events
        if self.threshold_exceeded[consumer_id] >= 3:
            print(
                "[CheckConsumerThroughputProcess] - Stopping after multiple throughput below tolerance...")
            self.stop()

    def run(self):
        print("[CheckConsumerThroughputProcess] - started.")
        self.check_throughput()
        print("[CheckConsumerThroughputProcess] - ended.")

