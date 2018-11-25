# Kafka-to-cassandra-and-mysql
A pipeline to take streaming data from twitter to cassandra and mysql using kafka

## Getting Started
As you know, zookeeper is necessary to run kafka, so let us first [download](https://www.apache.org/dyn/closer.cgi/zookeeper/) and untar zookeeper:
```
tar xvzf zookeeper-3.4.10.tar.gz
```


[Download](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz) Apache Kafka and untar it:
```
tar xvzf kafka_2.11-2.0.0.tgz
```
Likewise, download producer_consumer_threading.py from this project.

From the kafka folder, start a zookeeper server:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
and a kafka server:
```
bin/kafka-server-start.sh config/server.properties
```
Now, create a kafka topic named 'twitter':
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter
```

Finally, execute producer_consumer_threading.py using python 3 and you are good to go :)
