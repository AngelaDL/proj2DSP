package main.java.operators.query1;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static main.java.config.Configuration.*;

public class KafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private KafkaConsumer<String, String> consumer;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._collector = spoutOutputCollector;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_PORT);
        properties.put("group.id", GLOBAL_GROUP_ID);
        properties.put("enable.auto.commit", "true");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC_1_INPUT));
    }

    @Override
    public void nextTuple() {

        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(100);
            for (ConsumerRecord<String, String> rec : recs) {
                Values values = new Values();
                values.add(rec.value());
                values.add(System.currentTimeMillis());

                _collector.emit(values);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(RAW_DATA, CURRENT_TIMESTAMP));
    }
}
