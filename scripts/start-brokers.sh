#!/bin/bash
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.sh

BROKER_COUNT=$1

echo "start brokers, BROKER_COUNT=$BROKER_COUNT"

kubectl -n kafka patch -f - --kubeconfig ./scripts/kubeconfig.yaml<<EOF
kind: StatefulSet
apiVersion: apps/v1
metadata:
  name: kafka
  namespace: kafka
spec:
  replicas: $BROKER_COUNT
EOF