#!/bin/bash
PRODUCER_COUNT=$1
# echo "scale-producers, PRODUCER_COUNT=$PRODUCER_COUNT"

source ./export-gcp-credentials.sh

kubectl -n producer-consumer scale deployments producer --replicas=$PRODUCER_COUNT --kubeconfig ./kubeconfig.yaml
