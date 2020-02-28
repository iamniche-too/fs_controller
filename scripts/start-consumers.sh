#!/bin/bash
source ./export-gcp-credentials.sh
./generate-cluster-connection-yaml.sh

MESSAGE_SIZE_KB = $1

echo "start consumers, MESSAGE_SIZE_KB=" MESSAGE_SIZE_KB