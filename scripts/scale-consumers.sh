#!/bin/bash
NUM_CONSUMERS=$1

# echo "configure consumers, NUM_CONSUMERS=$NUM_CONSUMERS"

source ./export-gcp-credentials.sh

kubectl -n producer-consumer scale deployments consumer --replicas=$NUM_CONSUMERS --kubeconfig ./kubeconfig.yaml
