#!/bin/bash
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.sh

MESSAGE_SIZE_KB=$1

echo "start consumers, MESSAGE_SIZE_KB="$MESSAGE_SIZE_KB