export PYTHONPATH=`pwd`
source ./scripts/export-gcp-credentials.sh
./scripts/generate-cluster-connection-yaml.sh
python controller.py
