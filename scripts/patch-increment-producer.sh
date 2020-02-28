#!/bin/bash
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.sh

PRODUCER_COUNT = $1
MESSAGE_SIZE_KB = $2

echo "patch-increment-brokers, PRODUCER_COUNT=$PRODUCER_COUNT, MESSAGE_SIZE_KB=$MESSAGE_SIZE_KB

#kubectl -n producer-consumers patch -f - --kubeconfig ./kubeconfig.yaml<<EOF
#apiVersion: apps/v1
#kind: Deployment
#metadata:
#  name: producer
#  namespace: producer-consumer
#spec:
#  replicas: $PRODUCER_COUNT
#EOF