export PYTHONPATH=`pwd`
source ./scripts/export-gcp-credentials.sh

# pass in the argument too
python run.py $1
