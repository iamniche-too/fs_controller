#terraform init
./generate-kafka-node-pool.sh $1 $2 $3
terraform apply -auto-approve
