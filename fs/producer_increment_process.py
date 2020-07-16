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

    def run(self):
        desired_producer_count = self.configuration["start_producer_count"]
        while not self.is_stopped() and (desired_producer_count <= self.configuration["max_producer_count"]):
            i = 1
            producer_count = self.get_producer_count()
            attempts = (desired_producer_count - producer_count) * 40
            while not self.is_stopped() and producer_count != desired_producer_count:
                time.sleep(5)
                producer_count = self.get_producer_count()
                print(f"producer_count={producer_count} - still waiting for all producer(s) to start... ({i}/{attempts})")
                i += 1
                if i > attempts:
                    print("Error: Timeout waiting for producer(s) to start...")
                    self.stop()
                    break

            desired_producer_count += 1

            # Wait for a specified interval before starting the next producer
            producer_increment_interval_sec = self.configuration["producer_increment_interval_sec"]
            print(f"Waiting {producer_increment_interval_sec}s before starting next producer.")
            time.sleep(producer_increment_interval_sec)

            print(f"Starting next producer...")
            self.k8s_scale_producers(str(desired_producer_count))
