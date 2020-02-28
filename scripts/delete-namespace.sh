#!/bin/bash
source ./export-gcp-credentials.sh
./generate-cluster-connection-yaml.sh

NAMESPACE=$1

echo "delete namespace " $1

#kubectl delete ns $1 --kubeconfig ./kubeconfig.yaml