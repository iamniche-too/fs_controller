#!/bin/bash
source ./export-gcp-credentials.sh

kubectl get serviceaccounts -o json --kubeconfig=./kubeconfig.yaml > out.json
