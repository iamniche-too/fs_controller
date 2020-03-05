#!/bin/bash
NAMESPACE=$1
echo "delete namespace, NAMESPACE=$NAMESPACE"
kubectl delete ns $1 --kubeconfig ./kubeconfig.yaml
