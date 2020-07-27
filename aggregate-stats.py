import csv
import glob
import json
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

    def run(self):
        run_log_path = os.path.join(self.path, "log", "run_*", "run_*.log")
        run_log_list = self.get_run_logs(run_log_path)
        print(f"Found {len(run_log_list)} run logs.")

        i = 0
        for directory in run_log_list:
            # strip the path down to the filename
            file_name = os.path.basename(directory)
            # strip the .log
            run_uid = file_name[:-4]
            print(f"Found run_uid: {run_uid}")
            if i == 0:
                self.parse_files_per_run(self.path, run_uid, first_entry=True)
            else:
                self.parse_files_per_run(self.path, run_uid)
            i += 1

    def get_run_logs(self, path):
        file_list = [file_path for file_path in glob.glob(path, recursive=True)]
        return file_list

    def parse_files_per_run(self, path, run_uid, first_entry=False):
        # get the list of files
        file_list = self.get_metrics_files_per_run(path, run_uid)
        print(f"Found {len(file_list)} files for run_uid {run_uid}.")

        i = 0
        for file in file_list:
            print(f"Reading file {file}")

            with open(file, 'r', encoding='utf-8-sig') as input_file:
                data = json.load(input_file)
                print(data)

                output_file = os.path.join(self.output_directory, "results.csv")

                mode = "a"
                if first_entry:
                    mode = "w"

                with open(output_file, mode) as output_file:
                    for configuration_uid in data.keys():
                        d = data[configuration_uid]
                        d = dict({"run_uid": run_uid, "configuration_uid": configuration_uid}, **d)
                        w = csv.DictWriter(output_file, d.keys())

                        # write out the headers
                        if i == 0:
                            w.writerow(dict((fn, fn) for fn in d.keys()))

                        w.writerow(d)
                        print(f"Wrote entry for {configuration_uid}")

                    output_file.write("\n")
            i += 1

    def get_metrics_files_per_run(self, path, run_uid):
        glob_path = os.path.join(path, "log", run_uid, "*_metrics.csv")
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
