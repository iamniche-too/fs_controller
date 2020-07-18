import time
from collections import defaultdict
from statistics import mean

from fs.base_process import BaseProcess
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE

INITIAL_WINDOW_SIZE = 10


class ThroughputProcess(BaseProcess):

    def __init__(self, configuration, queue):
        super().__init__()
        self.configuration = configuration
        self.consumer_throughput_queue = queue
        self.threshold_exceeded = {}
        # initialise empty dict with empty dict of lists
        self.consumer_throughput_dict = defaultdict(lambda: defaultdict(list))
        self.previous_num_producers = 0

    def throughput_ok(self, consumer_id):
        # above threshold, reset the threshold events
        # (as they must be consecutive to stop the thread)
        print(f"[ThroughputProcess] - Consumer {consumer_id} average throughput ok.")
        self.threshold_exceeded[consumer_id] = 0

        # default is not to break out of the loop
        return False

    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        print(
            f"[ThroughputProcess] - Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}")
        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

    def break_loop(self):
        # default is not to break loop
        return False

    def check_throughput(self, window_size=INITIAL_WINDOW_SIZE):

        while not self.is_stopped():
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                time.sleep(.10)
                continue

            consumer_id = data["consumer_id"]
            throughput = data["throughput"]
            num_producers = data["producer_count"]

            # append throughput to specific list (as keyed by num_producers
            self.consumer_throughput_dict[consumer_id][num_producers].append(throughput)

            actual_producer_count = self.get_producer_count()

            if not self.configuration["ignore_throughput_threshold"] and num_producers == actual_producer_count:
                # detect threshold event if relevant to actual producer count
                if len(self.consumer_throughput_dict[consumer_id][actual_producer_count]) >= window_size:
                    # truncate list to last x entries
                    self.consumer_throughput_dict[consumer_id][actual_producer_count] = self.consumer_throughput_dict[consumer_id][actual_producer_count][-window_size:]

                    # calculate the mean
                    consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id][actual_producer_count])
                    print(
                        f"[ThroughputProcess] - Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                    consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                    if consumer_throughput_average < consumer_throughput_tolerance:
                        self.throughput_tolerance_exceeded(consumer_id, consumer_throughput_average, consumer_throughput_tolerance)
                    else:
                        self.throughput_ok(consumer_id)

                        if self.break_loop():
                            break
