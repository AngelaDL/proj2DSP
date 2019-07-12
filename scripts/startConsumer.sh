#!/bin/bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic query-1-output --from-beginning --bootstrap-server localhost:9092
#$KAFKA_HOME/bin/kafka-console-consumer.sh --topic query-2-output --from-beginning --bootstrap-server localhost:9092
