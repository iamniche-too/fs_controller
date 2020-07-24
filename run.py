import argparse
import greenstalk
from fs.default_controller import DefaultController
from fs.message_size_controller import MessageSizeController
from fs.partition_count_controller import PartitionCountController
from fs.replication_factor_controller import ReplicationFactorController


def run():
    parser = argparse.ArgumentParser(description='FS Kafka Trial Controller')
    parser.add_argument('--controller',
                        type=str,
                        help="The controller to run. Default=DefaultController",
                        required=False
                        )
    args = parser.parse_args()

    consumer_throughput_queue = greenstalk.Client(host='127.0.0.1', port=12000, watch='consumer_throughput')

    if args.controller is None:
        print("Starting All Controllers consecutively")

        print("1. Starting Message Size Controller")
        c = MessageSizeController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()

        print("2. Starting Partition Count Controller")
        c = PartitionCountController(consumer_throughput_queue)
        c.flush_consumer_throughput_queue()
        c.run()

        print("3. Starting Replication Factor Controller")
        c = ReplicationFactorController(consumer_throughput_queue)
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
    else:
        print(f"Did not understand argument {args.controller}, aborting...")


if __name__ == '__main__':
    run()
