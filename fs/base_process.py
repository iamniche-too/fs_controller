import os
import subprocess
from datetime import datetime
import json
import greenstalk

from fs.utils import SCRIPT_DIR, addlogger
from fs.stoppable_process import StoppableProcess


@addlogger
class BaseProcess(StoppableProcess):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_directory = os.path.dirname(os.path.abspath(__file__))
        self.now = datetime.now()

    def get_metrics_features(self):
        """
        Default implementation is an empty list
        :return:
        """
        return []

    def write_metrics(self, configuration, data):
        """
        Write JSON to metrics file

        :param configuration:
        :param data:
        :return:
        """
        base_path = os.path.join(self.base_directory, "..", "log", self.now.strftime("%Y-%m-%d"), configuration["run_uid"])

        # create path if not exist
        if not os.path.exists(base_path):
            os.makedirs(base_path)

        # merge any desired features to the dict
        metrics_features = self.get_metrics_features()
        for feature in metrics_features:
            d = {}
            try:
                d[feature] = self.configuration[feature]
            except KeyError:
                self.__log.error(f"Missing feature {feature} in configuration dict.")

        data = {**data, **d}

        metrics_filename = "{0}_metrics.csv".format(configuration["configuration_uid"])
        metrics_file = os.path.join(base_path, metrics_filename)
        with open(metrics_file, 'a') as outfile:
            json.dump(data, outfile)

    """
    Base process
    """
    def get_data(self, consumer_throughput_queue, timeout=1):
        data = None

        try:
            job = consumer_throughput_queue.reserve(timeout)

            if job is None:
                # self.__log("Nothing on consumer throughput queue...")
                return None

            data = json.loads(job.body)

            # Finally delete from queue
            try:
                consumer_throughput_queue.delete(job)
            except OSError as e:
                self.__log.warning(f"Warning: unable to delete job {job}, {e}", e)

        except greenstalk.TimedOutError:
            # mute for output sake
            # self.__log("Warning: nothing in consumer throughput queue.")
            pass
        except greenstalk.UnknownResponseError:
            self.__log.warning("Warning: unknown response from beanstalkd server.")
        except greenstalk.DeadlineSoonError:
            self.__log.warning("Warning: job timeout in next second.")
        except ConnectionError as ce:
            self.__log.error(f"Error: ConnectionError: {ce}")

        return data

    def bash_command_with_output(self, additional_args, working_directory):
        args = ['/bin/bash', '-e'] + additional_args
        # self.__log.info(args)
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


