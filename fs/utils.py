K8S_SERVICE_COUNT = 22

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "/scripts"
TERRAFORM_DIR = "/terraform/"

# set this to where-ever fs-kafka-k8s is cloned
# KAFKA_DEPLOY_DIR = "/home/nicholas/workspace/fs-kafka-k8s/"
KAFKA_DEPLOY_DIR = "/data/open-platform-checkouts/fs-kafka-k8s"
# PRODUCERS_CONSUMERS_DEPLOY_DIR = "/home/nicholas/workspace/fs-producer-consumer-k8s"
PRODUCERS_CONSUMERS_DEPLOY_DIR = "/data/open-platform-checkouts/fs-producer-consumer-k8s"
BURROW_DIR = "/data/open-platform-checkouts/fs-burrow-k8s"

DEFAULT_CONSUMER_TOLERANCE = 0.85
DEFAULT_THROUGHPUT_MB_S = 75

# Cluster restarted: 13/07
SERVICE_ACCOUNT_EMAIL = "cluster-minimal-e2b28d411d6b@kafka-k8s-trial.iam.gserviceaccount.com"

CLUSTER_NAME = "gke-kafka-cluster"
CLUSTER_ZONE = "europe-west2-a"

ENDPOINT_URL = "http://focussensors.duckdns.org:9000/consumer_reporting_endpoint"