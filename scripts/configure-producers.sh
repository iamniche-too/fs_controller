#!/bin/bash
MESSAGE_SIZE_KB=$1

echo "configure producers, MESSAGE_SIZE_KB=$MESSAGE_SIZE_KB"

source ./export-gcp-credentials.sh

kubectl -n producer-consumer apply -f - --kubeconfig ./kubeconfig.yaml<<EOF
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer
  namespace: producer-consumer
spec:
  selector:
    matchLabels:
      app: producer
  template:
      metadata:
        labels:
          app: producer
      spec:
        containers:
          - name: producer
            env:
            - name: MESSAGE_SIZE_KB
              value: "$MESSAGE_SIZE_KB"
EOF
