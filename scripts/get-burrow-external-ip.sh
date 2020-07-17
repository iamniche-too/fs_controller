#!/bin/bash
source ./export-gcp-credentials.sh
kubectl -n kafka get services -o jsonpath='{$.items[0].status.loadBalancer.ingress[0].ip}' --kubeconfig ./kubeconfig.yaml
