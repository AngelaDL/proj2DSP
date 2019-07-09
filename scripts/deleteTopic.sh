#!/bin/bash
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-input
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-1-output
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-2-input
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query-2-output