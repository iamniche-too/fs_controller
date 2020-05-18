#!/bin/bash
source ./export-gcp-credentials.sh
kubectl -n kafka get pods -o json --field-selector=status.phase==Running --kubeconfig ./kubeconfig.yaml | jq '.items[].metadata.name | select (. | startswith("zookeeper"))' | wc -w
