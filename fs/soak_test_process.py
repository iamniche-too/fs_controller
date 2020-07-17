import time
from statistics import mean
from collections import defaultdict
from fs.base_process import BaseProcess

# A 21GB pagefile can cache:
# 168Gb / (0.59 * p / n) == 285 * n / p seconds of data.
# 110% = 313s
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE

SOAK_TEST_S = 313


class SoakTestProcess(BaseProcess):
    """
    Soak test process
    """
    def __init__(self, configuration, queue):
        super().__init__()

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

    def run(self):
        # decrement the number of running producers
        self.decrement_producer_count()

        # decrement the producers further until stability is achieved
        stable = False
        while not self.is_stopped() and not stable:
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                # print("Nothing on consumer throughput queue...")
                time.sleep(.10)
                continue

            consumer_id = data["consumer_id"]
            throughput = data["throughput"]
            num_producers = data["producer_count"]

            # append to specific list (as stored in dict)
            self.consumer_throughput_dict[consumer_id].append(throughput)

            if len(self.consumer_throughput_dict[consumer_id]) >= 5:
                # truncate list to last 5 entries
                self.consumer_throughput_dict[consumer_id] = self.consumer_throughput_dict[consumer_id][-5:]

                consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id])
                print(f"[SoakTestProcess] - Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                if consumer_throughput_average < consumer_throughput_tolerance:
                    print(
                        f"[SoakTestProcess] - Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}")
                    self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

                    # check for consecutive threshold events
                    if self.threshold_exceeded[consumer_id] >= 3:
                        print("[SoakTestProcess] - Threshold (still) exceeded: decrementing producer count...")
                        self.decrement_producer_count()
                        self.threshold_exceeded[consumer_id] = 0
                else:
                    # reset the threshold events (since they must be consecutive to force an event)
                    self.threshold_exceeded[consumer_id] = 0

                # Check each consumer to see if we are stable
                is_stable = True
                for consumer_id in self.threshold_exceeded.keys():
                    # if there have been any threshold events, then it is not yet stable
                    if self.threshold_exceeded[consumer_id] > 0:
                        is_stable = False

                if is_stable:
                    break

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
        while not self.is_stopped() and ((time.time() - start_time_ms) <= soak_test_ms):
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                # print("Nothing on consumer throughput queue...")
                time.sleep(.10)
                continue
            print("[SoakTestProcess] - Received data from consumer throughput queue.")

        print(f"[SoakTestProcess] - Soak test complete after {soak_test_ms/1000} seconds.")
