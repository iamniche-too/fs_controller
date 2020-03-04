# fs_controller

Steps to set-up the controller:

1) Install and run the queue daemon (Beanstalkd)

```shell
sudo apt-get install beanstalkd
beanstalkd -l 127.0.0.1 -p 12000
```

```shell
pipenv shell
pipenv install --dev
```

```shell
./run.sh
```

Steps to run a configuraion

1) Provision the infrastructure (minus kafka node pool)
```shell
cd fs-terraform-cluster
terraform init
cd gcp
./provision.sh
./provision.sh (if firewall fails)
```

2) Deploy Zoo / Kafka broker configuration (which won't create any pods yet since the taints/tolerations will mean Zoo/Kafka are "stuck as "Pending")
```shell
cd fs-kafka-k8s
./deploy/gcp/deploy.sh
./watch-pods.sh
```

Note - all pods should be in status "Pending" at this point (since node pool is still missing)

3) Start beanstalkd
```shell
sudo apt-get install beanstalkd
beanstalkd -l 127.0.0.1 -p 12000
```

4) Run the configuration(s)

Note - this will provision the Kafka/Zoo node pool

```shell
pipenv shell
pipenv install --dev
./run.sh
```
