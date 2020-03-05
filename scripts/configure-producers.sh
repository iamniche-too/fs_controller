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
  replicas: 0
  selector:
    matchLabels:
      app: producer
  template:
    metadata:
      labels:
        app: producer
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values: ["kafka"]
              topologyKey: "kubernetes.io/hostname"
              namespaces:
                - kafka
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values: ["consumer"]
              topologyKey: "kubernetes.io/hostname"
      initContainers:
        - name: git-repo
          image: alpine/git
          args:
            - clone
            - --
            - https://github.com/jezaustin/fs-python
            - /etc/git-repo
          securityContext:
            runAsUser: 0
          volumeMounts:
            - name: git-repo-rw
              mountPath: /etc/git-repo
      containers:
        - name: producer
          image: nichemley/fs-producer-consumer-image
          args: ['/bin/bash', '/etc/config/start-producer.sh']
          volumeMounts:
            - name: git-repo-rw
              mountPath: /etc/git-repo
            - name: git-repo-ro
              mountPath: /etc/config
          env:
            - name: MESSAGE_SIZE_KB
              value: "$MESSAGE_SIZE_KB"
      volumes:
        - name: git-repo-rw
          emptyDir: {}
        - name: git-repo-ro
          configMap:
            name: init-producer
status: {}
EOF