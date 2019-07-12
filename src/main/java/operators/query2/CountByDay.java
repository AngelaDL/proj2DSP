package main.java.operators.query2;


import main.java.utils.DateUtils;
import main.java.utils.FileWriter;
import main.java.utils.SlotBasedWindow;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static main.java.config.Configuration.*;

public class CountByDay extends BaseRichBolt {

    private OutputCollector _collector;
    private SlotBasedWindow window;
    private long lastTick;
    private KafkaProducer<String, String> producer;
    private int stat;
    private long responseTime;
    private long nResponseTime;
    private long throughput;
    private long currentTime;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.window = new SlotBasedWindow();
        this.lastTick = 0;
        this.stat = 0;
        this.responseTime = 0;
        this.nResponseTime = 0;
        this.throughput = 0;
        this.currentTime = 0;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_PORT);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<String, String>(properties);

    }

    @Override
    public void execute(Tuple tuple) {

        this.stat += 1;

        String msgType = tuple.getSourceStreamId();


        // When a tick by metronome is received, it handles the window shifting operations
        if (msgType.equals(METRONOME_D_STREAM_ID)) {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            long timestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
            Date date = DateUtils.getDate(tupleTimestamp);

            if(this.stat == 1) {
                window.setIndex(tupleTimestamp);
                this.currentTime = System.currentTimeMillis();
            }

            //quando mi arriva il tick di un giorno dal metronomo, posso produrre i risultati
            if(tupleTimestamp > this.lastTick) {
                //calcolo quanto tempo è trascorso
                int elapsedHour = (int) Math.ceil((tupleTimestamp - lastTick) / (1000*60));
                long[] windowSize = window.getTimeframes();

                String result = "";
                result = result.concat(String.valueOf(date));
                result = result.concat(" [ ");
                for(int i = 0; i < windowSize.length; i ++) {
                    result += windowSize[i] + " ";
                }
                System.err.println("RESULT: " + result + "]");

                FileWriter fw = new FileWriter();
                try {
                    fw.writeResult("count_by_d_q2_p1", result);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                producer.send(new ProducerRecord<String, String>(TOPIC_2_OUTPUT, result));

                // Avanzo la finestra
                //this.window = new SlotBasedWindow();
                this.window.moveForward(elapsedHour);

                // Aggiorno il timestamp
                this.lastTick = tupleTimestamp;
            }

            this.throughput += 1;
            long ts = tuple.getLongByField(CREATE_DATE);
            updateMetrics(ts);
        }

        // When a msg from parser is received, it handles memorization operations in the window
        else {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);

            if(this.stat == 1) {
                window.setIndex(tupleTimestamp);
                this.currentTime = System.currentTimeMillis();
            }
            window.updateSlot(tupleTimestamp);

            long ts = System.currentTimeMillis() - tuple.getLongByField(CURRENT_TIMESTAMP);
            responseTime += ts;
            nResponseTime++;

        }

    }

    public void updateMetrics(long timestamp){
        double res = (double) 100 / (double) (System.currentTimeMillis() - currentTime);

        this.currentTime = System.currentTimeMillis();

        this.throughput = 0;

        System.out.println("Throughput daily query 2: " + res);
        FileWriter fw2 = new FileWriter();
        try {
            fw2.writeResult("thr_count_by_d_q2_p1", String.valueOf(res));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(nResponseTime == 0) {
            nResponseTime = 1;
        }
        double avgResponseTime = (double) responseTime / (double) nResponseTime;
        responseTime = 0;
        nResponseTime = 0;

        System.out.println("Response Time Daily query 2" + avgResponseTime);
        FileWriter fw3 = new FileWriter();
        try {
            fw3.writeResult("respT_count_by_d_q2_p1", String.valueOf(avgResponseTime));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
