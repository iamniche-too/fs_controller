#!/bin/bash
kubectl get serviceaccounts -o json --kubeconfig=./kubeconfig.yaml > out.json
