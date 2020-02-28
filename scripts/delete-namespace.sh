#!/bin/bash
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.s

NAMESPACE=$1

echo "delete namespace, NAMESPACE=$NAMESPACE"

kubectl delete ns $1 --kubeconfig ./kubeconfig.yaml