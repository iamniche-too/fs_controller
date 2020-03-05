#!/bin/bash
source ./export-gcp-credentials.sh

kubectl -n producer-consumer get pods -o json --field-selector=status.phase==Running --kubeconfig ./kubeconfig.yaml | jq '.items[].metadata.name | select (. | startswith("consumer"))' | wc -w
