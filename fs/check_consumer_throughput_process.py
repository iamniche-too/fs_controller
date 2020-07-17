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
    def throughput_tolerance_exceeded(self, consumer_id):
        """
        Throughput below tolerance

        :param consumer_id:
        :return:
        """
        super().throughput_tolerance_exceeded(consumer_id)

        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1
        print(
            f"[CheckConsumerThroughputProcess] - Warning: Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}, # exceptions {self.threshold_exceeded[consumer_id]}")

        # stop after 3 consecutive threshold events
        if self.threshold_exceeded[consumer_id] >= 3:
            print(
                "[CheckConsumerThroughputProcess] - Stopping after multiple throughput below tolerance...")
            self.stop()

    def run(self):
        print("[CheckConsumerThroughputProcess] - started.")
        self.check_throughput()
        print("[CheckConsumerThroughputProcess] - ended.")

