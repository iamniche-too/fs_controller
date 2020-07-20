import argparse
import logging
import os
from datetime import datetime

import greenstalk

from fs.default_controller import DefaultController
from fs.message_size_controller import MessageSizeController
from fs.partition_count_controller import PartitionCountController
from fs.replication_factor_controller import ReplicationFactorController


def configure_logging():
    # log directory
    base_directory = os.path.dirname(os.path.abspath(__file__))
    now = datetime.now()
    path = os.path.join(base_directory, "..", "log", now.strftime("%Y-%m-%d"))

    if not os.path.exists(path):
        os.makedirs(path)

    log_formatter = logging.Formatter("%(asctime)s [%(name)s] [%(levelname)-5.5s]  %(message)s")
    root_logger = logging.getLogger("fs")
    root_logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler("{0}/{1}.log".format(path, self.run_uid))
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)


def run():
    parser = argparse.ArgumentParser(description='FS Kafka Trial Controller')
    parser.add_argument('--controller',
                        type=str,
                        help="The controller to run. Default=DefaultController",
                        required=False
                        )
    args = parser.parse_args()

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

    configure_logging()

    if args.controller is None:
        print("Starting Default Controller")
        c = DefaultController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()
        return
    elif args.controller == "message-size":
        print("Starting Message Size Controller")
        c = MessageSizeController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()
        return
    elif args.controller == "partition-count":
        print("Starting Partition Count Controller")
        c = PartitionCountController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()
        return
    elif args.controller == "replication-factor":
        print("Starting Replication Factor Controller")
        c = ReplicationFactorController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()
        return


if __name__ == '__main__':
    run()
