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

    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        """
        Check for throughput below tolerance

        :param consumer_id:
        :return:
        """
        self.log(
            f"Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}")
        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

        # stop after 3 consecutive threshold events
        if self.threshold_exceeded[consumer_id] >= 3:
            self.log("Stopping after multiple throughput below tolerance...")
            # we want to quit due to the tolerance event
            return True

        # only quit if >= 3 tolerance events
        return False

    def start_new_producer(self, now, actual_producer_count):
        # store the time the new producer was started
        self.last_producer_start_time = now
        self.desired_producer_count = actual_producer_count + 1
        self.log(f"Starting producer, actual_producer_count {actual_producer_count}, desired_producer_count {self.desired_producer_count}")
        self.k8s_scale_producers(self.desired_producer_count)

    def throughput_ok(self, consumer_id, actual_producer_count):
        super().throughput_ok(consumer_id, actual_producer_count)

        # if interval has elapsed, then start a new producer
        now = time.time()
        elapsed_time = now - self.last_producer_start_time
        increment_time = self.configuration["producer_increment_interval_sec"]
        # self.log(f"time since last increment {elapsed_time}, increment_time {increment_time}")
        actual_producer_count = self.get_producer_count()
        if elapsed_time > increment_time:
            self.start_new_producer(now, actual_producer_count)

        return False

    def run(self):
        self.log("started.")

        # store the time that the thread is started
        self.last_producer_start_time = time.time()

        stop = False
        while not stop:
            stop = self.check_throughput(window_size=5)

        actual_producer_count = self.get_producer_count()
        if self.desired_producer_count > actual_producer_count:
            self.log("cancelling outstanding producer increment.")
            # cancel the outstanding increment
            self.k8s_scale_producers(actual_producer_count)

        self.log("ended.")

