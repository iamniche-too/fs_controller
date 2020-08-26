import csv
import glob
import os

from fs.read_write_jsonl_mixin import ReadWriteJSONLMixin

# TODO - think abou tthis a bit more: move to command line parameter or remove?
INCLUDED_RUN_UIDS = ["816229", "AFD15B", "C69AD1"]


class AggregateStats(ReadWriteJSONLMixin):

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

            if run_uid[4:] in INCLUDED_RUN_UIDS:
                print(f"Processing run_uid: {run_uid}")
                self.parse_files_per_run(self.path, run_uid)
            else:
                print(f"Ignoring run_uid {run_uid}")

    def get_run_logs(self, path):
        file_list = [file_path for file_path in glob.glob(path, recursive=True)]
        return file_list

    def parse_files_per_run(self, path, run_uid):
        # get the list of files
        file_list = self.get_metrics_files_per_run(path, run_uid)
        print(f"Found {len(file_list)} files for run_uid {run_uid}.")

        results = []
        for file in file_list:
            single_dict_per_file = {}

            print(f"Reading file {file}")
            data = self.load_jsonl(file)
            print(f"Found {len(data)} rows in file {file}.")

            # print(data)
            if len(data) == 0:
                print("Warning: no data rows in file {file}")
                continue

            # add run_uid to data
            data[0]["run_uid"] = run_uid

            if len(data) == 2:
                # merge to a single dict
                single_dict_per_file = dict(data[0], **data[1])
            else:
                print(f"Warning: Expected 2 rows, but actually <>2 rows in file {file}")
                continue

            # print(single_dict_per_file)
            results.append(single_dict_per_file)

        output_file = os.path.join(self.output_directory, "{0}_results.csv".format(run_uid))
        write_headers = True
        with open(output_file, "w") as output_file:
            print(results)

            for data_dict in results:
                w = csv.DictWriter(output_file, data_dict.keys())

                # write out the headers
                if write_headers:
                    w.writerow(dict((fn, fn) for fn in data_dict.keys()))
                    write_headers = False

                w.writerow(data_dict)

    def get_metrics_files_per_run(self, path, run_uid):
        glob_path = os.path.join(path, "log", run_uid, "*_metrics*.csv")
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
