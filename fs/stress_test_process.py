import time
from fs.throughput_process import ThroughputProcess
from fs.utils import addlogger, SEVENTY_FIVE_MBPS_IN_GBPS


@addlogger
class StressTestProcess(ThroughputProcess):
    """
    Stress test:
    a) Check throughput for tolerance
    b) Start a new producer if everything is tickety boo and interval has elapsed
    """
    def __init__(self, configuration, queue, *args, **kwargs):
        super().__init__(configuration, queue, *args, **kwargs)

        self.initial_producer_count = self.configuration["start_producer_count"]
        self.desired_producer_count = self.initial_producer_count

        self.last_producer_start_time = 0
        self.stress_test_max_producers = 0

    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        """
        Check for throughput below tolerance

        :param consumer_id:
        :return:
        """
        self.__log.info(
            f"Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}")
        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

        # stop after 3 consecutive threshold events
        if self.threshold_exceeded[consumer_id] >= 3:
            self.__log.info("Stopping after multiple throughput below tolerance...")
            # we want to quit due to the tolerance event
            return True

        # only quit if >= 3 tolerance events
        return False

    def throughput_ok(self, consumer_id, actual_producer_count):
        # above threshold, reset the threshold events
        # (as they must be consecutive to stop the thread)
        self.__log.info(
            f"Consumer {consumer_id} average throughput ok, expected {SEVENTY_FIVE_MBPS_IN_GBPS * actual_producer_count}")
        self.threshold_exceeded[consumer_id] = 0

        # if interval has elapsed, then start a new producer
        now = time.time()
        elapsed_time = now - self.last_producer_start_time
        increment_time = self.configuration["producer_increment_interval_sec"]
        # self.__log.info(f"time since last increment {elapsed_time}, increment_time {increment_time}")
        actual_producer_count = self.get_producer_count()
        if elapsed_time > increment_time:
            # store the time the new producer was started
            self.last_producer_start_time = now
            self.desired_producer_count = actual_producer_count + 1
            self.__log.info(
                f"Starting producer, actual_producer_count {actual_producer_count}, desired_producer_count {self.desired_producer_count}")
            self.k8s_scale_producers(self.desired_producer_count)

        return False

    def run(self):
        self.__log.info("Started.")

        # store the time that the thread is started
        self.last_producer_start_time = time.time()

        stop = False
        while not stop:
            # 8 data points = (8 * 5) = 40s of data
            stop = self.check_throughput(window_size=8)

        actual_producer_count = self.get_producer_count()
        if self.desired_producer_count > actual_producer_count:
            self.__log.info("cancelling outstanding producer increment.")
            # cancel the outstanding increment
            self.k8s_scale_producers(actual_producer_count)

        # write out the key metrics as JSON
        json = {"run_uid": self.configuration["run_uid"],
                "configuration_uid": self.configuration["configuration_uid"],
                "stress_max_producers": str(actual_producer_count),
                "stress_expected_throughput_gbps": str(actual_producer_count * SEVENTY_FIVE_MBPS_IN_GBPS),
                "stress_min_throughput": str(self.min_throughput),
                "stress_max_throughput": str(self.max_throughput)}
        self.__log.info(f"Stress test stats: {json}")
        self.write_metrics(self.configuration, json)

        self.__log.info("Completed.")

