#!/bin/bash
source ./export-gcp-credentials.sh

kubectl -n producer-consumer get pods -o json --kubeconfig ./kubeconfig.yaml | jq '.items[].metadata.name | select (. | startswith("producer"))' | wc -w
