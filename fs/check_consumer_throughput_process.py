import threading
import time
from statistics import mean
from collections import defaultdict

from fs.base_process import BaseProcess
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE


class CheckConsumerThroughputProcess(BaseProcess):
    """
    Check consumer throughput process
    """
    def __init__(self, configuration, queue):
        super().__init__()
        self.configuration = configuration
        self.consumer_throughput_queue = queue
        self.threshold_exceeded = {}
        self.consumer_throughput_dict = defaultdict(list)
        self.previous_num_producers = 0

    def run(self):
        # Ignore the first entry for all consumers
        consumer_dict = defaultdict()
        while len(consumer_dict.keys()) < int(self.configuration["num_consumers"]):
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                continue
            print(f"Ignoring data {data} on consumer throughput queue...")
            consumer_dict[data["consumer_id"]] = 1

        # continue reading from queue
        while not self.is_stopped():
            data = self.get_data(self.consumer_throughput_queue)
            if data is None:
                # print("Nothing on consumer throughput queue...")
                time.sleep(.10)
                continue

            consumer_id = data["consumer_id"]
            throughput = data["throughput"]
            num_producers = data["producer_count"]

            # detect if the num_producers has changed
            if self.previous_num_producers != num_producers:
                # A new producer has started, therefore clear the throughput entries for all consumers
                # to avoid "incorrect" degradation reports
                for key in self.consumer_throughput_dict.keys():
                    self.consumer_throughput_dict[key].clear()
                print("Flushed consumer throughput values.")

                self.previous_num_producers = num_producers

            if not self.configuration["ignore_throughput_threshold"]:
                # detect ANY consumer that has throughput below a threshold

                # append to specific list (as stored in dict)
                self.consumer_throughput_dict[consumer_id].append(throughput)

                if len(self.consumer_throughput_dict[consumer_id]) >= 5:
                    # truncate list to last x entries
                    self.consumer_throughput_dict[consumer_id] = self.consumer_throughput_dict[consumer_id][-5:]

                    # calculate the mean
                    consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id])
                    print(f"Consumer {consumer_id} throughput (average) = {consumer_throughput_average}")

                    consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                    if consumer_throughput_average < consumer_throughput_tolerance:
                        # below threshold
                        print(f"Warning: Consumer {consumer_id} average throughput {consumer_throughput_average} < tolerance {consumer_throughput_tolerance}, # exceptions {self.threshold_exceeded[consumer_id]}")
                        self.threshold_exceeded[consumer_id] = self.threshold_exceeded.get(consumer_id, 0) + 1

                        # stop after 3 consecutive threshold events
                        if self.threshold_exceeded[consumer_id] >= 3:
                            print("Stopping after multiple throughput below tolerance...")
                            self.stop()
                    else:
                        # above threshold, reset the threshold events
                        # (as they must be consecutive to stop the thread)
                        self.threshold_exceeded[consumer_id] = 0

