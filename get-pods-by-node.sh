kubectl get pods --all-namespaces -o wide --field-selector spec.nodeName=<node> --kubeconfig=./kubeconfig.yaml
