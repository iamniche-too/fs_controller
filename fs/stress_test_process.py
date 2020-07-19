import time
from statistics import mean
from collections import defaultdict

from fs.base_process import BaseProcess
from fs.throughput_process import ThroughputProcess
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE


class StressTestProcess(ThroughputProcess):
    """
    Stress test:
    a) Check throughput for tolerance
    b) Start a new producer if everything is tickety boo and interval has elapsed
    """
    def __init__(self, configuration, queue):
        super().__init__(configuration, queue)

        self.initial_producer_count = self.configuration["start_producer_count"]
        self.desired_producer_count = self.initial_producer_count
        self.last_producer_start_time = 0

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
            print("[StressTestProcess] - Stopping after multiple throughput below tolerance...")
            return False

        return True

    def start_new_producer(self, now):
        # store the time the new producer was started
        self.last_producer_start_time = now
        actual_producer_count = self.get_producer_count()
        self.desired_producer_count = actual_producer_count + 1
        print(f"[StressTestProcess] - Starting producer, actual_producer_count {actual_producer_count}, desired_producer_count {self.desired_producer_count}")
        self.k8s_scale_producers(self.desired_producer_count)

    def throughput_ok(self, consumer_id):
        # above threshold, reset the threshold events
        # (as they must be consecutive to stop the thread)
        print(f"[StressTestProcess] - Consumer {consumer_id} average throughput ok.")
        self.threshold_exceeded[consumer_id] = 0

        # if interval has elapsed, then start a new producer
        now = time.time()
        if (now - self.last_producer_start_time) > (self.configuration["producer_increment_interval_sec"]*1000):
            self.start_new_producer(now)

        return False

    def run(self):
        print("[StressTestProcess] - started.")

        # store the time that the thread is started
        self.last_producer_start_time = time.now()

        stop = False
        while not stop:
            stop = self.check_throughput()

        print("[StressTestProcess] - ended.")

