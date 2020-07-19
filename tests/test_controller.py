import json
import time
import unittest
import greenstalk

from fs.controller import Controller, StressTestProcess, SoakTestProcess


# IMPORTANT: Please set the directory to be the root directory NOT /tests/
class TestController(unittest.TestCase):

    # TODO - currently requires ./run_beanstalkd.sh to be running locally...
    def setUp(self):
        # Note - need to "use" AND "watch"
        self.queue = greenstalk.Client(host='127.0.0.1', port=12000, use='consumer_throughput', watch='consumer_throughput')
        self.controller = Controller(self.queue)

    def tearDown(self):
        self.queue.close()

    def test_run_soak_test(self):
        self.controller.flush_consumer_throughput_queue()

        self.queue.put(json.dumps({'consumer_id': 'AAAAA', 'throughput': 75, 'producer_count': 1}))
        self.queue.put(json.dumps({'consumer_id': 'BBBBB', 'throughput': 75, 'producer_count': 1}))
        self.queue.put(json.dumps({'consumer_id': 'CCCCC', 'throughput': 75, 'producer_count': 1}))

        configuration = {
            "configuration_uid": "68ED2AB",
            "number_of_brokers": 3, "message_size_kb": 750, "start_producer_count": 3, "max_producer_count": 9,
            "num_consumers": 3,
            "producer_increment_interval_sec": 180, "machine_size": "n1-highmem-2", "disk_size": 100,
            "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5}

        soak_test_process = SoakTestProcess(configuration, self.queue)

        # start the thread for soak test
        soak_test_process.start()

        time.sleep(10)

        soak_test_process.stop()

    def test_run_configuration(self):
        self.controller.flush_consumer_throughput_queue()

        self.queue.put(json.dumps({'consumer_id': 'AAAAA', 'throughput': 75, 'producer_count': 1}))
        self.queue.put(json.dumps({'consumer_id': 'BBBBB', 'throughput': 75, 'producer_count': 1}))
        self.queue.put(json.dumps({'consumer_id': 'CCCCC', 'throughput': 75, 'producer_count': 1}))

        configuration = {
            "configuration_uid": "68ED2AB",
            "number_of_brokers": 3, "message_size_kb": 750, "start_producer_count": 3, "max_producer_count": 9,
            "num_consumers": 3,
            "producer_increment_interval_sec": 180, "machine_size": "n1-highmem-2", "disk_size": 100,
            "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5}

        self.controller.run_configuration(configuration)

        time.sleep(10)

        self.controller.stop_threads()

    def test_consumer_throughput_queue(self):
        self.controller.flush_consumer_throughput_queue()

        self.queue.put(json.dumps({'consumer_id': 'AAAAA', 'throughput': 75, 'producer_count': 1}))
        self.queue.put(json.dumps({'consumer_id': 'AAAAA', 'throughput': 75, 'producer_count': 1}))
        self.queue.put(json.dumps({'consumer_id': 'AAAAA', 'throughput': 75, 'producer_count': 1}))

        try:
            job1 = self.queue.reserve()
            job2 = self.queue.reserve()
            job3 = self.queue.reserve()

            print(job1)
            self.assertTrue(job1.id > 0)
            print(job2)
            self.assertTrue(job2.id > 0)
            print(job3)
            self.assertTrue(job3.id > 0)

            self.queue.delete(job1)
            self.queue.delete(job2)
            self.queue.delete(job3)
        except greenstalk.TimedOutError:
            self.assertTrue(False)

        try:
            job4 = self.queue.reserve(timeout=1)
            self.assertTrue(False)
        except greenstalk.TimedOutError:
            pass

    def test_flush_consumer_throughput_queue(self):
        self.queue.put(json.dumps({"consumer_id": "AAAAA", "throughput": 0.01, "producer_count": 1}))
        self.queue.put(json.dumps({"consumer_id": "BBBBB", "throughput": 0.01, "producer_count": 1}))
        self.queue.put(json.dumps({"consumer_id": "CCCCC", "throughput": 0.01, "producer_count": 1}))
        self.controller.flush_consumer_throughput_queue()

        try:
            self.queue.reserve(timeout=1)
            self.assertTrue(False)
        except greenstalk.TimedOutError:
            # should time out...
            pass

    def test_check_consumer_throughput(self):
        self.controller.flush_consumer_throughput_queue()

        print("Sending data to queue...")

        # send throughput to the queue - should be ignored
        id1 = self.queue.put(json.dumps({"consumer_id": "AAAAA", "throughput": 0.01, "producer_count": 1}))
        id2 = self.queue.put(json.dumps({"consumer_id": "BBBBB", "throughput": 0.01, "producer_count": 1}))
        id3 = self.queue.put(json.dumps({"consumer_id": "CCCCC", "throughput": 0.01, "producer_count": 1}))

        # simulate ok throughput
        for i in range(1, 10, 1):
            self.queue.put(json.dumps({"consumer_id": "AAAAA", "throughput": 75, "producer_count": 1}))
            self.queue.put(json.dumps({"consumer_id": "BBBBB", "throughput": 75, "producer_count": 1}))
            self.queue.put(json.dumps({"consumer_id": "CCCCC", "throughput": 75, "producer_count": 1}))

        # simulate degraded throughput
        for i in range(1, 10, 1):
            self.queue.put(json.dumps({"consumer_id": "AAAAA", "throughput": 60, "producer_count": 1}))
            self.queue.put(json.dumps({"consumer_id": "BBBBB", "throughput": 75, "producer_count": 1}))
            self.queue.put(json.dumps({"consumer_id": "CCCCC", "throughput": 75, "producer_count": 1}))

        print("Finished sending data to queue...")

        configuration = {
            "configuration_uid": "68ED2AB",
            "number_of_brokers": 3, "message_size_kb": 750, "start_producer_count": 3, "max_producer_count": 9,
            "num_consumers": 3,
            "producer_increment_interval_sec": 180, "machine_size": "n1-highmem-2", "disk_size": 100,
            "disk_type": "pd-ssd", "consumer_throughput_reporting_interval": 5}

        # run as process
        process = StressTestProcess(configuration, self.queue)
        process.start()
        process.join()