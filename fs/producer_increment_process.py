import time

from fs.base_process import BaseProcess


class ProducerIncrementProcess(BaseProcess):
    """
    Producer increment process
    """
    def __init__(self, configuration, consumer_throughput_process):
        super().__init__()
        self.configuration = configuration
        self.consumer_throughput_process = consumer_throughput_process

    def wait_interval(self):
        # Wait for a specified interval before starting the next producer
        producer_increment_interval_sec = self.configuration["producer_increment_interval_sec"]
        print(f"[ProducerIncrementProcess] - Waiting {producer_increment_interval_sec}s before starting next producer.")
        time.sleep(producer_increment_interval_sec)

    def check_producer_count(self, desired_producer_count):
        actual_producer_count = self.get_producer_count()
        while not self.is_stopped() and actual_producer_count < desired_producer_count:
            time.sleep(5)
            actual_producer_count = self.get_producer_count()
            print(f"[ProducerIncrementProcess] - actual_producer_count={actual_producer_count}, desired_producer_count={desired_producer_count}")

        print(f"[ProducerIncrementProcess] - actual_producer_count={actual_producer_count}")

    def run(self):
        initial_producer_count = self.configuration["start_producer_count"]
        self.check_producer_count(initial_producer_count)
        self.wait_interval()

        # increment the producer count
        desired_producer_count = initial_producer_count
        while not self.is_stopped() and (desired_producer_count < self.configuration["max_producer_count"]):
            desired_producer_count += 1

            if not self.is_stopped():
                print(f"[ProducerIncrementProcess] - Starting producer, desired_producer_count={desired_producer_count}")
                self.k8s_scale_producers(desired_producer_count)

            self.check_producer_count(desired_producer_count)
            self.wait_interval()
