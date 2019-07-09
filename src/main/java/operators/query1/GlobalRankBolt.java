package main.java.operators.query1;

import main.java.utils.DateUtils;
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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static main.java.config.Configuration.*;

public class GlobalRankBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private KafkaProducer<String, String> producer;
    private TopKRanking topKranking;
    private int k;
    private String kafkaTopic;

    public GlobalRankBolt(int k) {
        this.k = k;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes")Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(k);

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_PORT);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void execute(Tuple tuple) {

        boolean updated = false;
        long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
        long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
        String metronomeMsg = tuple.getStringByField(TIME_ID);

        Ranking partialRanking = (Ranking) tuple.getValueByField(PARTIAL_RANKING);

        for (RankItem item : partialRanking.getRanking()) {
            updated |= topKranking.update(item);
            //System.out.println(updated);
        }

        if (updated) {
            //System.out.println("Sono entrato");
            createOutputResponse(currentTimestamp, tupleTimestamp);
        }

        _collector.ack(tuple);
    }

    private void createOutputResponse(long currentTimestamp, long tupleTimestamp) {

        Date date = DateUtils.getDate(tupleTimestamp);
        List<RankItem> globalRanking = topKranking.getTopK().getRanking();

        //String result = tupleTimestamp + ", ";
        String result = "";
        result = result.concat(String.valueOf(date));

        for(int i = 0; i < globalRanking.size(); i++) {
            RankItem item = globalRanking.get(i);
            result = result + ", " + item.getArticleID() + ", " + item.getPopularity();
        }

        System.err.println("RESULT: " + result);

        if(globalRanking.size() < k){
            int i = k - globalRanking.size();
            for(int j = 0; j < i; j++){
                result += "NULL";
                result += ", ";
            }
        }

        producer.send(new ProducerRecord<String, String>(TOPIC_1_OUTPUT, result));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
