#!/bin/bash
kubectl -n kafka get pods -o json --kubeconfig ./scripts/kubeconfig.yaml | jq '.items[].metadata.name | select (. | startswith("kafka"))' | wc -w
