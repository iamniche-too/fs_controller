import json
import subprocess
import time

import greenstalk

from fs.utils import SCRIPT_DIR
from fs.stoppable_process import StoppableProcess


class BaseProcess(StoppableProcess):
    """
    Base process
    """
    def get_data(self, consumer_throughput_queue, timeout=1):
        data = None

        try:
            job = consumer_throughput_queue.reserve(timeout)

            if job is None:
                # print("Nothing on consumer throughput queue...")
                return None

            data = json.loads(job.body)

            # Finally delete from queue
            try:
                consumer_throughput_queue.delete(job)
            except OSError as e:
                print(f"Warning: unable to delete job {job}, {e}", e)

        except greenstalk.TimedOutError:
            # mute for output sake
            # print("[BaseProcess] - Warning: nothing in consumer throughput queue.")
        except greenstalk.UnknownResponseError:
            print("[BaseProcess] - Warning: unknown response from beanstalkd server.")
        except greenstalk.DeadlineSoonError:
            print("[BaseProcess] - Warning: job timeout in next second.")
        except ConnectionError as ce:
            print(f"[BaseProcess] - Error: ConnectionError: {ce}")

        return data

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        print(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, cwd=working_directory)
        p.wait()
        out = p.communicate()[0].decode("UTF-8")
        return out

    def k8s_scale_producers(self, producer_count):
        filename = "./scale-producers.sh"
        args = [filename, str(producer_count)]
        self.bash_command_with_output(args, SCRIPT_DIR)

    def get_producer_count(self):
        filename = "./get-producers-count.sh"
        args = [filename]

        producer_count = 0
        try:
            producer_count = int(self.bash_command_with_output(args, SCRIPT_DIR))
        except ValueError:
            pass

        return producer_count
