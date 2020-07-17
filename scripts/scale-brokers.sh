#!/bin/bash
BROKER_COUNT=$1

# echo "start brokers, BROKER_COUNT=$BROKER_COUNT"

source ./export-gcp-credentials.sh

kubectl -n kafka scale statefulsets kafka --replicas=$BROKER_COUNT --kubeconfig ./kubeconfig.yaml
