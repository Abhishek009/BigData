#########Create kafka topic#########
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper sandbox.hortonworks.com:2181 --replication-factor 1 --partitions 1 --topic movie

#########List Kafka topic#########
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox.hortonworks.com:2181 --list 

#########Describe Kafka topic #########
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper sandbox.hortonworks.com:2181 --topic movie

#########Delete Kafka topic#########
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox.hortonworks.com:2181 --delete --topic movie

#########Kafka Producer#########
sh /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic movie

#########Kafka Consumer#########
sh /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic movie --from-beginning

#########Change the replication factor of a topic#########
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox.hortonworks.com:2181 --alter --topic movie --replication-factor 3

######################Increase kafka partition######################
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --alter --zookeeper sandbox.hortonworks.com:2181 --topic movie --partition 2

#########To get the largest offset###########
sh /usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list sandbox.hortonworks.com:6667 --time -1 --topic movie

#########To get the smallest offset###########
sh /usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list sandbox.hortonworks.com:6667 --time -2 --topic movie

#########Rebalance kafka############
sh /usr/hdp/current/kafka-broker/bin/kafka-preferred-replication-election.sh --zookeeper sandbox.hortonworks.com:2181

#########Retention time change############
Property: retention.ms
Default: 7days
server default property: log.retention.minutes
sh /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox.hortonworks.com:2181 --alter --topic <topicname> --config retention.ms=86400000









