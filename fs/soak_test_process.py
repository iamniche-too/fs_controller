import time
from statistics import mean
from collections import defaultdict

# A 21GB pagefile can cache:
# 168Gb / (0.59 * p / n) == 285 * n / p seconds of data.
# 110% = 313s
from fs.throughput_process import ThroughputProcess
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE

SOAK_TEST_S = 313


class SoakTestProcess(ThroughputProcess):
    """
    Soak test process
    """
    def __init__(self, configuration, queue):
        super().__init__(configuration, queue)

        self.configuration = configuration
        self.consumer_throughput_queue = queue
        self.consumer_throughput_dict = defaultdict(list)
        self.threshold_exceeded = {}

    def decrement_producer_count(self):
        producer_count = self.get_producer_count()
        print(f"[SoakTestProcess] - Current producer count is {producer_count}")

        if producer_count > 0:
            # decrement the producer count
            producer_count -= 1

            # decrement the producer count
            self.k8s_scale_producers(producer_count)

            print(f"[SoakTestProcess] - Decrementing the producer count to {producer_count}")
        else:
            print("[SoakTestProcess] - Producer count is zero.")

    def throughput_ok(self, consumer_id):
        super().throughput_ok(consumer_id)

        # Check each consumer to see if we are stable
        is_stable = True
        for consumer_id in self.threshold_exceeded.keys():
            # if there have been any threshold events, then it is not yet stable
            if self.threshold_exceeded[consumer_id] > 0:
                is_stable = False

        return is_stable

    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        super().throughput_tolerance_exceeded(consumer_id, consumer_throughput_average, consumer_throughput_tolerance)

        # check for consecutive threshold events
        if self.threshold_exceeded[consumer_id] >= 3:
            print("[SoakTestProcess] - Threshold exceeded.")

            num_producers = self.get_producer_count()

            # decrement the producer count
            self.decrement_producer_count()

            # wait for the producer count to settle
            self.check_producer_count(num_producers - 1)

            # reset the thresholds
            self.threshold_exceeded[consumer_id] = 0

            # reset the throughput list for this particular consumer
            self.consumer_throughput_dict[consumer_id].clear()

    def run(self):
        print("[SoakTestProcess] - started.")

        self.check_throughput()

        num_producers = self.get_producer_count()
        print(f"[SoakTestProcess] - Throughput stability achieved @ {num_producers} producers.")

        # start soak test once stability achieved
        num_brokers = self.configuration["number_of_brokers"]
        if num_producers > 0:
            soak_test_ms = ((SOAK_TEST_S * num_brokers) / num_producers) * 1000
            print(f"[SoakTestProcess] - Running soak test for {soak_test_ms/1000} seconds.")
        else:
            print("[SoakTestProcess] - No producers: aborting soak test...")
            self.stop()
            return

        start_time_ms = time.time()
        run_time_ms = 0
        while not self.is_stopped():
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                # print("Nothing on consumer throughput queue...")
                time.sleep(.10)
                continue

            consumer_id = data["consumer_id"]
            throughput = data["throughput"]
            self.consumer_throughput_dict[consumer_id].append(throughput)
            consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id][-5:])

            print(
                f"[SoakTestProcess] - {run_time_ms/1000}s of {soak_test_ms/1000}s Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

            # update the timings
            run_time_ms = time.time() - start_time_ms
            if run_time_ms > soak_test_ms:
                break

        print(f"[SoakTestProcess] - Soak test complete after {soak_test_ms/1000} seconds.")
