#!/bin/bash
kubectl -n producer-consumer get pods -o json --kubeconfig ./scripts/kubeconfig.yaml | jq '.items[].metadata.name | select (. | startswith("consumer"))' | wc -w
