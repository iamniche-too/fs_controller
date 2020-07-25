import glob
import os

INCLUDED_METRICS_FILES = ["C30E5C"]


class AggregateStats:

    def __init__(self):
        self.path = os.path.dirname(os.path.abspath(__file__))

        # spit out the aggregated csv
        self.output_directory = os.path.join(self.path, "results")

        # create path if not exist
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        self.output_file = os.path.join(self.output_directory, "results.csv")
        with open(self.output_file, "w") as file:
            # write out the headers
            file.write(
                "run_uid,configuration_uid,stress_max_producers,stress_max_gbps,soak_num_producers,soak_throughput_gbps,soak_min_throughput,soak_max_throughput,soak_average_throughput\n")

    def run(self):
        run_log_path = os.path.join(self.path, "log/**", "run_*.log")
        run_log_list = self.get_run_logs(run_log_path)
        print(f"Found {len(run_log_list)} run logs.")

        for directory in run_log_list:
            # strip the path down to the filename
            file_name = os.path.basename(directory)
            # strip the .log
            run_uid = file_name[:-4]
            print(f"Found run_uid: {run_uid}")
            self.parse_files_per_run(self.path, run_uid)

    def get_run_logs(self, path):
        file_list = [file_path for file_path in glob.glob(path, recursive=True)]
        return file_list

    def parse_files_per_run(self, path, run_uid):
        # get the list of files
        file_list = self.get_metrics_files_per_run(path, run_uid)
        print(f"Found {len(file_list)} files for run_uid {run_uid}.")

        # for each file, load the stats
        for file in file_list:
            print(f"Reading file {file}")

            with open(file, 'r', encoding='utf-8-sig') as input_file:
                i = 1

                stress_test_results = None
                soak_test_results = None
                for line in self.nonblank_lines(input_file):
                    if i == 1:
                        stress_test_results = self.parse_first_line(line)
                        # print(stress_test_results)
                    elif i == 2:
                        soak_test_results = self.parse_second_line(line)
                        # print(soak_test_results)
                    i += 1

                if not soak_test_results:
                    soak_test_results = {}
                    # missing, so set all to None
                    keys = ['soak_num_producers','soak_throughput_gbps','soak_min_throughput','soak_max_throughput','soak_average_throughput']
                    d = {}
                    for key in keys:
                        d[key] = None

                    configuration_uid = next(iter(stress_test_results))
                    soak_test_results[configuration_uid] = d

                aggregated_results = self.merge_dictionaries(stress_test_results, soak_test_results)
                # print(aggregated_results)

                output_file = os.path.join(self.output_directory, "results.csv")
                with open(output_file, "a") as file:
                    for configuration_uid in aggregated_results.keys():
                        file.write("{0},{1},{2},".format(run_uid, configuration_uid,
                                                                aggregated_results[configuration_uid]["stress_max_producers"]))

                        file.write("{0},{1},{2},".format(aggregated_results[configuration_uid]["stress_max_gbps"],
                                                        aggregated_results[configuration_uid]["soak_num_producers"],
                                                        aggregated_results[configuration_uid]["soak_throughput_gbps"]))

                        file.write("{0},{1},{2}\n".format(aggregated_results[configuration_uid]["soak_min_throughput"],
                                                        aggregated_results[configuration_uid]["soak_max_throughput"],
                                                        aggregated_results[configuration_uid]["soak_average_throughput"]))

    def parse_first_line(self, first_line):
        # CONFIGURATION_UID,STRESS_MAX_PRODUCERS,STRESS_MAX_GBPS=4D46A3,9,5.3999999999999995
        items = first_line.split("=")
        values = items[1]
        values_list = values.split(",")
        results = {values_list[0]: {"stress_max_producers": values_list[1], "stress_max_gbps": values_list[2]}}
        return results

    def parse_second_line(self, second_line):
        # CONFIGURATION_UID,SOAK_NUM_PRODUCERS,SOAK_THROUGHPUT_GBPS,SOAK_MIN_THROUGHPUT_GBPS,SOAK_MAX_THROUGHPUT_GBPS,SOAK_AVERAGE_THROUGHPUT_GBPS=BE307A,9,5.3999999999999995,0.5429271014400031,4.782042163049214
        items = second_line.split("=")
        values = items[1]
        values_list = values.split(",")
        results = {values_list[0]: {"soak_num_producers": values_list[1], "soak_throughput_gbps": values_list[2], "soak_min_throughput": values_list[3], "soak_max_throughput": values_list[4], "soak_average_throughput": values_list[5]}}
        return results

    # only return non-blank lines from a file
    def nonblank_lines(self, file):
        for l in file:
            line = l.rstrip()
            if line:
                yield line

    def get_metrics_files_per_run(self, path, run_uid):
        glob_path = os.path.join(path, "log", "**", run_uid, "*_metrics.csv")
        # if self.includes(file_path)
        file_list = [file_path for file_path in glob.glob(glob_path, recursive=True)]
        return file_list

    def includes(self, file_path):
        include = False
        for file_name in INCLUDED_METRICS_FILES:
            if file_name in file_path:
                include = True
        return include

    def merge_dictionaries(self, d1, d2):
        d3 = {}
        for key in d1.keys():
            wanted = d1[key]
            wanted2 = d2[key]
            d3[key] = {**wanted, **wanted2}

        return d3


if __name__ == '__main__':
    a = AggregateStats()
    a.run()