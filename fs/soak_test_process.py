import time
from statistics import mean
from fs.utils import addlogger, SEVENTY_FIVE_MBPS_IN_GBPS

# A 21GB pagefile can cache:
# 168Gb / (0.59 * p / n) == 285 * n / p seconds of data.
# 110% = 313s
from fs.throughput_process import ThroughputProcess

SOAK_TEST_S = 313

@addlogger
class SoakTestProcess(ThroughputProcess):
    """
    Soak test process
    """
    def __init__(self, configuration, queue, *args, **kwargs):
        # for a soak test we assume the system is already running
        # i.e. do not discard initial values
        super().__init__(configuration, queue, discard_initial_values=False, *args, **kwargs)

        self.consumer_throughput_averages = []
        self.num_producers = 0

    def decrement_producer_count(self):
        actual_producer_count = self.get_producer_count()
        self.__log.info(f"Current producer count is {actual_producer_count}")

        if actual_producer_count > 0:
            # decrement the producer count
            self.desired_producer_count = actual_producer_count - 1

            # decrement the producer count
            self.k8s_scale_producers(self.desired_producer_count)

            self.__log.info(f"Decrementing the producer count to {self.desired_producer_count}")
        else:
            self.__log.info("Producer count is zero.")

    def reset_thresholds(self):
        actual_producer_count = super().reset_thresholds()

        # if producer count has changed i.e. been reduced, then update the desired count
        if self.desired_producer_count > actual_producer_count:
            self.desired_producer_count = actual_producer_count

    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        self.__log.info(
            f"Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}")
        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

        # check for consecutive threshold events
        if self.threshold_exceeded[consumer_id] >= 3:
            actual_producer_count = self.get_producer_count()
            self.__log.info(f"Threshold exceeded, actual_producer_count {actual_producer_count}, desired_producer_count {self.desired_producer_count}")

            if self.desired_producer_count == actual_producer_count:
                # only decrement the producer count if we haven't already done so
                self.decrement_producer_count()

        # we never want to quit due to tolerance events
        return False

    def throughput_ok(self, consumer_id, actual_producer_count):
        # above threshold, reset the threshold events
        # (as they must be consecutive to stop the thread)
        self.__log.info(
            f"Consumer {consumer_id} average throughput ok, expected {SEVENTY_FIVE_MBPS_IN_GBPS * actual_producer_count}")
        self.threshold_exceeded[consumer_id] = 0

        # Check each consumer to see if we are stable
        is_stable = True
        for consumer_id in self.threshold_exceeded.keys():
            # if there have been any threshold events, then it is not yet stable
            if self.threshold_exceeded[consumer_id] > 0:
                is_stable = False

        return is_stable

    def run(self):
        self.__log.info("Started.")

        # set the desired producer count at the beginning
        self.desired_producer_count = self.get_producer_count()

        stop = False
        while not stop:
            # 8 data points = (8 * 5) = 40s of data
            stop = self.check_throughput(window_size=8)

        num_producers = self.get_producer_count()
        self.__log.info(f"Throughput stability achieved @ {num_producers} producers.")

        # start soak test once stability achieved
        num_brokers = self.configuration["number_of_brokers"]
        if num_producers > 0:
            soak_test_ms = ((SOAK_TEST_S * num_brokers) / num_producers)
            self.__log.info(f"Running soak test for {soak_test_ms:.2f} seconds.")
        else:
            self.__log.info("No producers: aborting soak test...")
            self.stop()

            json = {"run_uid": self.configuration["run_uid"],
                    "configuration_uid": self.configuration["configuration_uid"],
                    "soak_num_producers": 0,
                    "soak_expected_throughput_gbps": 0,
                    "soak_min_throughput": 0,
                    "soak_max_throughput": 0,
                    "soak_average_throughput": 0}
            self.__log.info(f"Soak soak stats: {json}")
            self.write_metrics(self.configuration, json)
            return

        # reset min/max, as we are not interested in the values before this point
        self.min_throughput = 99999
        self.max_throughput = 0

        start_time_ms = time.time()
        run_time_ms = 0
        while not self.is_stopped():
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                # self.__log.info("Nothing on consumer throughput queue...")
                time.sleep(.10)
                continue

            consumer_id = data["consumer_id"]
            throughput_in_mbps = data["throughput"]
            throughput_in_gbps = (throughput_in_mbps * 8) / 1000
            num_producers = data["producer_count"]

            # store the num_producers
            if self.num_producers == 0:
                self.num_producers = num_producers

            self.consumer_throughput_dict[consumer_id][str(num_producers)].append(throughput_in_gbps)
            consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id][str(num_producers)][-5:])

            self.consumer_throughput_averages.append(consumer_throughput_average)

            if consumer_throughput_average < self.min_throughput:
                self.min_throughput = consumer_throughput_average

            if consumer_throughput_average > self.max_throughput:
                self.max_throughput = consumer_throughput_average

            self.__log.info(
                f"{run_time_ms:.2f}s of {soak_test_ms:.2f}s Consumer {consumer_id} throughput (average) {consumer_throughput_average}, expected {SEVENTY_FIVE_MBPS_IN_GBPS * num_producers}")

            # update the timings
            run_time_ms = time.time() - start_time_ms
            if run_time_ms > soak_test_ms:
                self.__log.info(f"Soak test complete after {soak_test_ms:.2f} s.")
                break

        average_throughput = mean(self.consumer_throughput_averages)
        self.__log.info(f"Soak test stats: num_producers {self.num_producers}, min_throughput {self.min_throughput}, max_throughput {self.max_throughput}, average_throughput {average_throughput}")

        # write metrics as JSON
        json = {"run_uid": self.configuration["run_uid"],
                "configuration_uid": self.configuration["configuration_uid"],
                "soak_num_producers": str(self.num_producers),
                "soak_expected_throughput_gbps": str(num_producers * SEVENTY_FIVE_MBPS_IN_GBPS),
                "soak_min_throughput": str(self.min_throughput),
                "soak_max_throughput": str(self.max_throughput),
                "soak_average_throughput": str(average_throughput)}
        self.__log.info(f"Soak test stats: {json}")
        self.write_metrics(self.configuration, json)

        self.__log.info(f"Completed.")
