#!/bin/bash
NUM_CONSUMERS=$1

echo "configure consumers, NUM_CONSUMERS=$NUM_CONSUMERS"

source ./export-gcp-credentials.sh

kubectl -n producer-consumer scale consumers --replicas=$NUM_CONSUMERS --kubeconfig ./kubeconfig.yaml
