## Kafka学习笔记

### 类介绍

Producer类：生产者，每次生产一条信息

ProducerWithLoop：生产者，循环生产消息

Consumer：消费者

KafkaProperties：一些常用的属性，如生产者和消费者指定的消息的topic，已经kafka集群的地址

### 常用命令

创建topic

–zookeeper 后面是zookeeper的地址

```
sh kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic myTopic
```

查看所有的topic

–zookeeper 后面是zookeeper的地址

```
sh kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic myTopic
```

在某个topic上面发送消息

–broker-list后面是一个kafka的集群，topic名称为myTopic

```
sh kafka-console-producer.sh --broker-list localhost:9092 --topic myTopic
```
