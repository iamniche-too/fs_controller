#!/bin/bash
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.sh

MESSAGE_SIZE_KB=$1

echo "start producers, MESSAGE_SIZE_KB=$MESSAGE_SIZE_KB"

kubectl -n producer-consumers patch -f - --kubeconfig ./scripts/kubeconfig.yaml<<EOF
template:
    metadata:
      labels:
        app: producer
    spec:
      containers:
        - name: producer
          env:
          - name: MESSAGE_SIZE_KB
            value: $MESSAGE_SIZE_KB
EOF
