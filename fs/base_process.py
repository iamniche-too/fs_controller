import json
import logging
import subprocess
from datetime import datetime

import greenstalk

from fs.utils import SCRIPT_DIR
from fs.stoppable_process import StoppableProcess


class BaseProcess(StoppableProcess):

    def __init__(self):
        super().__init__()

        # logging
        self.log_to_stdout = True
        self.external_logger = None

    """
    Base process
    """
    def get_data(self, consumer_throughput_queue, timeout=1):
        data = None

        try:
            job = consumer_throughput_queue.reserve(timeout)

            if job is None:
                # logging.info("Nothing on consumer throughput queue...")
                return None

            data = json.loads(job.body)

            # Finally delete from queue
            try:
                consumer_throughput_queue.delete(job)
            except OSError as e:
                logging.warn(f"Warning: unable to delete job {job}, {e}", e)

        except greenstalk.TimedOutError:
            # mute for output sake
            # logging.info("Warning: nothing in consumer throughput queue.")
            pass
        except greenstalk.UnknownResponseError:
            logging.warning("Warning: unknown response from beanstalkd server.", level="WARNING")
        except greenstalk.DeadlineSoonError:
            logging.warning("Warning: job timeout in next second.", level="WARNING")
        except ConnectionError as ce:
            logging.error(f"Error: ConnectionError: {ce}", level="ERROR")

        return data

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        # logging.info(args)
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

    def set_logger(self, logger):
        """
        Also log to something with a :method:`write`.
        e.g. StringIO
        """
        self.external_logger = logger

    def log(self, msg, level="INFO"):
        """
        @param level: (str) DEBUG, PROGRESS, INFO, WARNING, ERROR or CRITICAL
        """
        if not (self.log_to_stdout or self.external_logger is not None):
            return

        date_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        msg = "{} {} [{}] {}".format(date_str, level.ljust(10), type(self).__name__, msg)

        if self.external_logger is not None:
            self.external_logger.write(msg)

        if self.log_to_stdout:
            print(msg)

