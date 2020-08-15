import logging
import os

K8S_SERVICE_COUNT = 22

PRODUCER_CONSUMER_NAMESPACE = "producer-consumer"
KAFKA_NAMESPACE = "kafka"
SCRIPT_DIR = "./scripts/"
TERRAFORM_DIR = "./terraform/"

# TODO - use BASE_DIR consistently for all static variables
# TODO - use os.join.path consistently
BASE_DIR = "/data/open-platform-checkouts"
KAFKA_DEPLOY_DIR = "/data/open-platform-checkouts/fs-kafka-k8s"
PRODUCERS_CONSUMERS_DEPLOY_DIR = "/data/open-platform-checkouts/fs-producer-consumer-k8s"
BURROW_DIR = "/data/open-platform-checkouts/fs-burrow-k8s"
MONITORING_DIR = BASE_DIR + "/fs-monitoring-k8s"
LOCAL_PROVISIONER_DEPLOY_DIR = os.path.join(BASE_DIR, "fs-local-provisioner")

DEFAULT_CONSUMER_TOLERANCE = 0.90
DEFAULT_THROUGHPUT_MB_S = 75
SEVENTY_FIVE_MBPS_IN_GBPS = 0.6

# TODO - use a static Service Account rather than create one each time we provision the infra
SERVICE_ACCOUNT_EMAIL = "cluster-minimal-45719c44c5ec@kafka-k8s-trial.iam.gserviceaccount.com"

CLUSTER_NAME = "gke-kafka-cluster"
CLUSTER_ZONE = "europe-west2-a"

ENDPOINT_URL = "http://focussensors.duckdns.org:9000/consumer_reporting_endpoint"


def addlogger(cls: type):
    """
    Add a logger to the class.

    Log messages using self.__log.info(message)

    :param cls:
    :return:
    """
    aname = '_{}__log'.format(cls.__name__)
    setattr(cls, aname, logging.getLogger(cls.__module__ + '.' + cls.__name__))
    return cls
