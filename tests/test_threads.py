import threading
import time

import unittest


class TestThreads(unittest.TestCase):

    def increment_producers_thread(self, thread_number):
        print(f"Thread {thread_number}")
        actual_producer_count = 1
        while actual_producer_count <= 2:
            print(f"Starting producer {actual_producer_count}")
            time.sleep(15)
            actual_producer_count += 1

    def check_consumer_throughput(self, thread_number):
        print(f"Thread {thread_number}")
        while True:
            print(f"Consumer throughput queue...")
            time.sleep(5)

    def test_configuration(self):
        print(f"3. Running configuration.")

        thread1 = threading.Thread(target=self.increment_producers_thread, args=(1,))
        thread2 = threading.Thread(target=self.check_consumer_throughput, args=(1,))
        thread2.start()
        thread1.start()
        thread1.join()

        # no need to wait for second thread to finish
        # thread2.join()

        print("Run completed.")
