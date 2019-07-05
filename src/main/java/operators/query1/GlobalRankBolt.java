package main.java.operators.query1;

import main.java.utils.RankItem;
import main.java.utils.Ranking;
import main.java.utils.TopKRanking;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static config.Configuration.*;

public class GlobalRankBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private KafkaProducer<String, String> producer;
    private TopKRanking topKranking;
    private int k;
    private boolean USE_KAFKA;
    private String kafkaTopic;

    public GlobalRankBolt(boolean USE_KAFKA, int k, String kafkaTopic) {
        this.USE_KAFKA = USE_KAFKA;
        this.k = k;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(k);

        if (this.USE_KAFKA) {
            Properties props = new Properties();
            props.put("bootstrap.servers", KAFKA_IP_PORT);
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", StringSerializer.class);

            producer = new KafkaProducer<String, String>(props);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        boolean updated = false;
        long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
        long currentTimestamp = tuple.getLongByField(CURRENNT_TIMESTAMP);
        //String metronomeMsg = tuple.getStringByField(METRONOME_H_STREAM_ID);
        //String articleID = tuple.getStringByField(PARSER_QUERY_1[1]);
        //long estimatedTotal = tuple.getLongByField(ESTIMATED_TOTAL);

        Ranking partialRanking = (Ranking) tuple.getValueByField(PARTIAL_RANKING);

        for (RankItem item : partialRanking.getRanking())
            updated |= topKranking.update(item);

        if (updated)
            createOutputResponse(currentTimestamp, tupleTimestamp);

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // I don't need to declare fields, cuz this is the final bolt :B
        //outputFieldsDeclarer.declare(new Fields("END"));
    }

    private void createOutputResponse(long currentTimestamp, long tupleTimestamp) {
        String result = "";
        List<RankItem> globalRanking = this.topKranking.getTopK().getRanking();

        result.concat(String.valueOf(tupleTimestamp)).concat(", ");

        for (int i=0; i< globalRanking.size(); i++)
            result.concat(globalRanking.get(i).getArticleID())
                   .concat(", ").concat(String.valueOf(globalRanking.get(i).getPopularity()));

        System.err.println("FINAL RESULT: " + result);

        producer.send(new ProducerRecord<String, String>(this.kafkaTopic, result));
    }
}
