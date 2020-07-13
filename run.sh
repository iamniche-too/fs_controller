export PYTHONPATH=`pwd`
source ./scripts/export-gcp-credentials.sh
python fs/default_controller.py
