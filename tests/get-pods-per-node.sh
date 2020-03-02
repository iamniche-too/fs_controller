source ./export-gcp-credentials.sh
./generate-cluster-connection-yaml.sh

#NODE_NAME=gke-gke-kafka-cluster-default-5b8fabf3-0c4f

#kubectl get pods --all-namespaces -o wide --field-selector spec.nodeName=$NODE_NAME --kubeconfig=./kubeconfig.yaml

kubectl -n kafka get pod -o=custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName --kubeconfig=./kubeconfig.yaml
kubectl -n producer-consumer get pod -o=custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName --kubeconfig=./kubeconfig.yaml
