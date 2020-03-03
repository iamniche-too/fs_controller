#!/bin/bash
kubectl get serviceaccounts -o json --kubeconfig=./scripts/kubeconfig.yaml > out.json
