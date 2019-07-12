package main.java.operators.query1;

import main.java.utils.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.*;

import static main.java.config.Configuration.*;

public class GlobalRankBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private KafkaProducer<String, String> producer;
    private TopKRanking topKranking;
    private int k;

    private int throughput;
    private long currentTime;
    private long latency;
    private long nLatency;

    public GlobalRankBolt(int k) {
        super();
        this.k = k;
        this.latency = 0;
        this.nLatency = 0;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(k);
        this.throughput = 0;
        this.currentTime = 0;

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_PORT);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void execute(Tuple tuple) {

        if (this.throughput == 0) {
            this.currentTime = System.currentTimeMillis();
        }

        long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
        long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
        String msgType = tuple.getStringByField(TIME_ID);


        Ranking partialRanking = (Ranking) tuple.getValueByField(PARTIAL_RANKING);

        /* update global rank */
        boolean updated = false;
        for (RankItem item : partialRanking.getRanking()) {
            updated |= topKranking.update(item);
            //System.out.println("PROVA: " + DateUtils.getDate(item.getTimestamp()));
            //System.out.println("PROVA: " + DateUtils.getDate(item.getTimestamp()) + ": " + item.getArticleID() + " " + item.getPopularity());
        }

        long ts = System.currentTimeMillis() - tuple.getLongByField(CURRENT_TIMESTAMP);
        latency += ts;
        nLatency++;

        /* Emit if the local top3 is changed */
        if (updated) {

            createOutputResponse(tupleTimestamp);

            this.throughput += 1;

            updateMetrics();

            this.throughput = 0;

        }

        _collector.ack(tuple);
    }



    private void createOutputResponse(long tupleTimestamp) {

        Date date = DateUtils.getDate(tupleTimestamp);
        List<RankItem> globalRanking = topKranking.getTopK().getRanking();

        String result = "";
        result = result.concat(String.valueOf(date)).concat(": ");


        for(int i = 0; i < globalRanking.size(); i++) {
            RankItem item = globalRanking.get(i);
            result += " " + item.getArticleID() + ", " + item.getPopularity();
        }

        System.err.println("RESULT: " + result);

        FileWriter fw = new FileWriter();
        try {
            fw.writeResult("Result_q1_month_p1", String.valueOf(result));
        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.send(new ProducerRecord<String, String>(TOPIC_1_OUTPUT, result));
    }


    public void updateMetrics(){

        double res = (double) 100 / (double) (System.currentTimeMillis() - this.currentTime);

        this.currentTime = System.currentTimeMillis();

        this.throughput = 0;

        System.out.println("Throughput query 1: " + res);
        FileWriter fw2 = new FileWriter();
        try {
            fw2.writeResult("thr_count_by_d_q1_p1", String.valueOf(res));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(nLatency == 0) {
            nLatency = 1;
        }

        double avgResponseTime = (double) latency / (double) nLatency;
        latency = 0;
        nLatency = 0;

        System.out.println("Response Time query 1: " + avgResponseTime);
        FileWriter fw3 = new FileWriter();
        try {
            fw3.writeResult("respT_count_by_d_q1_p1", String.valueOf(avgResponseTime));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
