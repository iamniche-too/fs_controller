from collections import defaultdict
from statistics import mean

from fs.base_process import BaseProcess
from fs.utils import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE, addlogger

INITIAL_WINDOW_SIZE = 10


@addlogger
class ThroughputProcess(BaseProcess):

    def __init__(self, configuration, queue, discard_initial_values=True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.configuration = configuration
        self.consumer_throughput_queue = queue
        self.threshold_exceeded = {}
        # initialise empty dict with empty dict of lists
        self.consumer_throughput_dict = defaultdict(lambda: defaultdict(list))
        self.previous_producer_count = 0

        # pertaining to discarding initial values (e.g. for stress test)
        self.discard_initial_values = discard_initial_values
        self.throughput_count = 0

        # min/max
        self.min_throughput = 99999
        self.max_throughput = 0

        self.desired_producer_count = 0

    def throughput_tolerance_exceeded(self, consumer_id, consumer_throughput_average, consumer_throughput_tolerance):
        raise NotImplementedError("Please use a sub-class to implement the method.")

    def reset_thresholds(self, consumer_id):
        actual_producer_count = self.get_producer_count()
        if self.previous_producer_count != actual_producer_count:
            self.__log.info(f"Producer change detected: previous {self.previous_producer_count}, current {actual_producer_count} - resetting thresholds.")
            self.threshold_exceeded[consumer_id] = 0
        self.previous_producer_count = actual_producer_count
        return actual_producer_count

    def check_throughput(self, window_size=INITIAL_WINDOW_SIZE):
        data = self.get_data(self.consumer_throughput_queue)
        if data is None:
            return False

        consumer_id = data["consumer_id"]
        throughput = data["throughput"]
        num_producers = data["producer_count"]

        # reset thresholds only if the producer count has changed
        # i.e. if a new producer has just started
        self.reset_thresholds(consumer_id)

        self.__log.info(f"Consumer {consumer_id}, throughput {throughput}, num_producers {num_producers}")

        # discard the data if the producer count doesn't match what we are expecting
        if self.desired_producer_count != num_producers:
            self.__log.info(f"num_producers {num_producers} != self.desired_producer_count {self.desired_producer_count}, producer not started/stopped yet? Discarding the data...")
            return False

        # only append to list if it is not an initial value (avoids low throughput when starting up)
        if self.discard_initial_values:
            if self.throughput_count > 4:
                # append throughput to specific list (as keyed by num_producers)
                self.consumer_throughput_dict[consumer_id][str(num_producers)].append(throughput)

                # update min/max, etc.
                if throughput < self.min_throughput:
                    self.min_throughput = throughput

                if throughput > self.max_throughput:
                    self.max_throughput = throughput
            else:
                self.__log.info("Discarding initial throughput value...")
                self.throughput_count += 1
        else:
            # append throughput to specific list (as keyed by num_producers)
            self.consumer_throughput_dict[consumer_id][str(num_producers)].append(throughput)

        if not self.configuration["ignore_throughput_threshold"]:
            # detect threshold event if relevant to actual producer count
            if len(self.consumer_throughput_dict[consumer_id][str(num_producers)]) >= window_size:
                # truncate list to last x entries
                self.consumer_throughput_dict[consumer_id][str(num_producers)] = self.consumer_throughput_dict[consumer_id][str(num_producers)][-window_size:]

                # calculate the mean
                consumer_throughput_average = mean(self.consumer_throughput_dict[consumer_id][str(num_producers)])
                self.__log.info(
                    f"Consumer {consumer_id}, throughput (average) = {consumer_throughput_average}, expected {DEFAULT_THROUGHPUT_MB_S * num_producers}")

                consumer_throughput_tolerance = (DEFAULT_THROUGHPUT_MB_S * num_producers * DEFAULT_CONSUMER_TOLERANCE)

                if consumer_throughput_average < consumer_throughput_tolerance:
                    return self.throughput_tolerance_exceeded(consumer_id, consumer_throughput_average, consumer_throughput_tolerance)
                else:
                    return self.throughput_ok(consumer_id, num_producers)

        return False

