# fs_controller

Steps to set-up the controller:

1) Install and run the queue daemon (Beanstalkd)

```shell
sudo apt-get install beanstalkd
./beanstalkd -l 127.0.0.1 -p 12000
```

```shell
pipenv shell
pipenv install --dev
```
