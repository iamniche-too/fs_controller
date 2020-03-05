#!/bin/bash
NAMESPACE=$1
echo "delete namespace, NAMESPACE=$NAMESPACE"

source ./export-gcp-credentials.sh

kubectl delete ns $1 --kubeconfig ./kubeconfig.yaml
