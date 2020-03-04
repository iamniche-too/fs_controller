import threading
import time
from statistics import mean

import greenstalk

from controller import DEFAULT_THROUGHPUT_MB_S, DEFAULT_CONSUMER_TOLERANCE


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper


class MockQueuePusher:
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, use='consumer_throughput')

    @threaded
    def run(self, post_interval, degraded_after_interval):
        print(
            f"[MockQueuePusher] Pushing to consumer_through_put_queue every {post_interval}s, with degradation after_{degraded_after_interval}s")
        iterations = degraded_after_interval / post_interval

        while True:
            i = 0
            while i < iterations:
                print(f"[MockQueuePusher] Pushing normal throughput to queue {DEFAULT_THROUGHPUT_MB_S}")
                self.consumer_throughput_queue.put(str(DEFAULT_THROUGHPUT_MB_S))
                time.sleep(post_interval)
                i += 1

            below_tolerance = DEFAULT_THROUGHPUT_MB_S * (DEFAULT_CONSUMER_TOLERANCE - 0.1)
            j = 0
            while j <= 2:
                print(f"[MockQueuePusher] Pushing degradation to queue {below_tolerance}")
                self.consumer_throughput_queue.put(str(below_tolerance))
                time.sleep(post_interval)
                j += 1


class MockQueuePoller:
    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

    @threaded
    def run(self, poll_interval):
        print(f"[MockQueuePoller] Polling consumer_throughput_queue every {poll_interval}s")

        throughput_list = []

        while True:
            try:
                job = self.consumer_throughput_queue.reserve()
                throughput = float(job.body)

                print(f"[MockQueuePoller] throughput={throughput}")

                # append to the list
                throughput_list.append(throughput)

                if len(throughput_list) >= 3:
                    # truncate list to last 3 entries
                    throughput_list = throughput_list[-3:]

                    consumer_throughput_average = mean(throughput_list)
                    print(f"Consumer throughput (average) = {consumer_throughput_average}")

                    tolerance = DEFAULT_THROUGHPUT_MB_S * DEFAULT_CONSUMER_TOLERANCE
                    if consumer_throughput_average < tolerance:
                        print(f"[MockQueuePoller] average throughput {consumer_throughput_average} below tolerance {tolerance}")

                self.consumer_throughput_queue.delete(job)

                time.sleep(poll_interval)
            except greenstalk.TimedOutError:
                pass


# For threading approach
# see https://stackoverflow.com/questions/19846332/python-threading-inside-a-class/19846691
if __name__ == '__main__':
    c = MockQueuePusher()
    #d = MockQueuePoller()
    handle1 = c.run(5, 15)
    #handle2 = d.run(5)
    handle1.join()
    #handle2.join()