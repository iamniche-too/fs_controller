#!/bin/bash
kubectl -n kafka get pods -o json --field-selector=status.phase==Running --kubeconfig ./scripts/kubeconfig.yaml | jq '.items[].metadata.name | select (. | startswith("kafka"))' | wc -w
