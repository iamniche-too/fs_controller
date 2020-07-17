#!/bin/bash
CLUSTER_NAME=$1
CLUSTER_ZONE=$2

# echo "configuring gcloud with CLUSTER_NAME=$CLUSTER_NAME, CLUSTER_ZONE=$CLUSTER_ZONE"

export GET_CMD="gcloud container clusters describe $CLUSTER_NAME --zone=$CLUSTER_ZONE"

cat <<EOF > ./kubeconfig.yaml
apiVersion: v1
kind: Config
current-context: my-cluster
contexts: [{name: my-cluster, context: {cluster: cluster-1, user: user-1}}]
users: [{name: user-1, user: {auth-provider: {name: gcp}}}]
clusters:
- name: cluster-1
  cluster:
    server: "https://$(eval "$GET_CMD --format='value(endpoint)'")"
    certificate-authority-data: "$(eval "$GET_CMD --format='value(masterAuth.clusterCaCertificate)'")"
EOF