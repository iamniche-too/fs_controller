#!/bin/bash
PRODUCER_COUNT = $1

echo "scale-producers, PRODUCER_COUNT=$PRODUCER_COUNT"

kubectl -n producer-consumer scale statefulsets producer --replicas=$PRODUCER_COUNT --kubeconfig ./scripts/kubeconfig.yaml
