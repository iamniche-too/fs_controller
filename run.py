import argparse
import greenstalk

from fs.batch_size_controller import BatchSizeController
from fs.consumer_controller import ConsumerController
from fs.debug_controller import DebugController
from fs.partition_count_controller import PartitionCountController
from fs.replication_factor_controller import ReplicationFactorController


def run():
    parser = argparse.ArgumentParser(description='FS Kafka Trial Controller')
    parser.add_argument('--controller',
                        type=str,
                        help="The controller to run. Default=DebugController",
                        required=False
                        )
    args = parser.parse_args()

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

    if args.controller == "batch-size":
        print("Starting Batch Size Controller")
        c = BatchSizeController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()
        return
    elif args.controller == "consumer":
        print("Starting Consumer Controller")
        c = ConsumerController(consumer_throughput_queue)
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
    elif args.controller == "debug":
        print("Starting Debug Controller")
        c = DebugController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()
        return
    else:
        print(f"Did not understand argument {args.controller}, aborting...")


if __name__ == '__main__':
    run()
