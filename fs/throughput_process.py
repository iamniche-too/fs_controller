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
        self.consumer_throughput_dict = defaultdict(list)
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

    def flush_throughputs(self):
        # A new producer has started, therefore clear the throughput entries for all consumers
        # to avoid "incorrect" degradation reports
        for key in self.consumer_throughput_dict.keys():
            self.consumer_throughput_dict[key].clear()

        print("[ThroughputProcess] - Flushed consumer throughput values.")

    def detect_producer_count_change(self, num_producers):
        # detect if the num_producers has changed
        # since if it has we want to flush the throughput values
        if self.previous_num_producers != num_producers:
            self.flush_throughputs()
            self.previous_num_producers = num_producers

    def ignore_data(self, num_producers):
        """
        Defalt implementation will never ignore

        :param num_producers:
        :return:
        """
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

            if self.ignore_data(num_producers):
                continue

            self.detect_producer_count_change(num_producers)

            if not self.configuration["ignore_throughput_threshold"]:
                # detect ANY consumer that has throughput below a threshold

                # append to specific list (as stored in dict)
                self.consumer_throughput_dict[consumer_id].append(throughput)

                if len(self.consumer_throughput_dict[consumer_id]) >= window_size:
                    # truncate list to last x entries
                    self.consumer_throughput_dict[consumer_id] = self.consumer_throughput_dict[consumer_id][-window_size:]

                    # calculate the mean
                    consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id])
                    print(
                        f"[ThroughputProcess] - Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                    consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                    if consumer_throughput_average < consumer_throughput_tolerance:
                        self.throughput_tolerance_exceeded(consumer_id, consumer_throughput_average, consumer_throughput_tolerance)
                    else:
                        self.throughput_ok(consumer_id)

                        if self.break_loop():
                            break
