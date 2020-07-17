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

    def wait(self):
        # Wait for a specified interval before starting the next producer
        producer_increment_interval_sec = self.configuration["producer_increment_interval_sec"]
        print(f"Waiting {producer_increment_interval_sec}s before starting next producer.")
        time.sleep(producer_increment_interval_sec)

    def run(self):
        initial_producer_count = self.configuration["start_producer_count"]

        actual_producer_count = self.get_producer_count()
        i = 1
        while not self.is_stopped() and actual_producer_count < initial_producer_count:
            time.sleep(5)
            actual_producer_count = self.get_producer_count()

            if i % 5 == 0:
                print(f"[ProducerIncrementProcess] - waiting for producer(s) to start, actual_producer_count={producer_count}, initial_producer_count={initial_producer_count}")

            i += 1

        self.wait()

        # begin incrementing
        while not self.is_stopped() and (desired_producer_count <= self.configuration["max_producer_count"]):
            desired_producer_count += 1

            if not self.is_stopped():
                print(f"[ProducerIncrementProcess] - Starting next producer...")
                self.k8s_scale_producers(str(desired_producer_count))

            self.wait()
