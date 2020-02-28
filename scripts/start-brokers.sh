#!/bin/bash
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.sh

BROKER_COUNT=$1

echo "start brokers, BROKER_COUNT="$BROKER_COUNT