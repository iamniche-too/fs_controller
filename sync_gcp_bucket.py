"""
Uses gsutil rsyc command to download data from GCP Bucket: gs://kafka-trial-data
"""
import os


class SyncGCPBuckets:

    @staticmethod
    def sync():
        """
        Sync the GCP bucket
        """
        gsutil_bin = "/snap/bin/gsutil" if os.access("/snap/bin/gsutil", os.X_OK) else "gsutil"

        base_directory = os.path.dirname(os.path.abspath(__file__))
        file_mappings = {
            "gs://kafka-trial-data": os.path.join(base_directory, "log")
        }

        for gcp_bucket, local_location in file_mappings.items():
            print(f"Processing... {gcp_bucket} into {local_location}")

            if not os.path.exists(local_location):
                os.makedirs(local_location)

            os.system(f'{gsutil_bin} -m rsync -d -r "{gcp_bucket}" "{local_location}"')


if __name__ == "__main__":
    SyncGCPBuckets.sync()
