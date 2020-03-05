#!/bin/bash
source ./export-gcp-credentials.sh
kubectl get pod --all-namespaces --field-selector=status.phase==Running -o json --kubeconfig=./kubeconfig.yaml | jq '.items[].metadata.name' | wc -w