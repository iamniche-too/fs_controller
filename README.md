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

# Configuring the number of Kafka brokers
1) Edit the controller.py configuration template, key "number_of_brokers": 5, "num_zk": 3
2) If broker # > 7, configure the kafka services in fs-kafka-k8s (by default there are 7 internal services)
3) Configure zookeeper in kafka config (/fs-kafka-k8s/kafka/kafka-config.yaml)
E.g.
zookeeper.connect=zookeeper-0.zookeeper-service.kafka.svc.cluster.local:2181,zookeeper-1.zookeeper-service.kafka.svc.cluster.local:2181,zookeeper-2.zookeeper-service.kafka.svc.cluster.local:2181

3) Configure zookeeper in zookeeper config (fs-kafka-k8s/zookeeper-3.14.14/zookeeper-config.yaml)
E.g.
server.0=zookeeper-0.zookeeper-service.kafka.svc.cluster.local:2888:3888:participant
server.1=zookeeper-1.zookeeper-service.kafka.svc.cluster.local:2888:3888:participant
server.2=zookeeper-2.zookeeper-service.kafka.svc.cluster.local:2888:3888:participant

4) Configure Burrow in fs-burrow-k8s/burrow-config.yaml
E.g.
servers=[ "zookeeper-0.zookeeper-service:2181", "zookeeper-1.zookeeper-service:2181", "zookeeper-2.zookeeper-service:2181" ]
and
servers=[ "kafka-0.kafka-service:9092", "kafka-1.kafka-service:9092", "kafka-2.kafka-service:9092", "kafka-3.kafka-service:9092", "kafka-4.kafka-service:9092"]
 
5) Push the code and check out on fsserver
6) Run it up! ;) 

# Aggregate the results

1) Sync the GCP bucket (to /log directory) to ensure that all data is present and correct
2) Remove any unwanted directories from /log directory
3) Remove any unwanted results files from /results directory
4) Configure aggregate-stats.py with the required run uids (line 8)
5) Run the aggregate stats from the command line
```shell script
./aggregate-stats.sh
``` 
6) Look for any warnings of <>2 rows in files (would indicate a problem with the run)

E.g. Warning: Expected 2 rows, but actually <>2 rows in file .../fs_controller/log/run_816229/20204E_metrics_5.csv

7) Open up the results/results.csv file and load into your favourite editor

