package main.java.operators.query2;

import main.java.operators.MetronomeBolt;
import main.java.utils.SlotBasedWindowWeek;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Properties;

import static main.java.config.Configuration.*;

public class CountByWeek extends BaseRichBolt {
    private OutputCollector _collector;
    private SlotBasedWindowWeek windowWeek;
    private long lastTick;
    private KafkaProducer<String, String> producer;
    private int status = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.windowWeek = new SlotBasedWindowWeek();
        this.lastTick = 0;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_IP_PORT);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void execute(Tuple tuple) {
        String msgType = tuple.getSourceStreamId();

        if(msgType.equals(METRONOME_D_STREAM_ID)) {
            System.out.println("lo status è: " + status);
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            long timestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
            System.err.println("TICK: " + msgType);

            if(tupleTimestamp > this.lastTick) {
                int elapsedDay = (int) Math.ceil((tupleTimestamp - lastTick) / MetronomeBolt.MILLIS_D);
                System.out.println("ELAPSED: " + elapsedDay);

                System.out.println("window 7 x 24");
                for (int i = 0; i < 7; i++) {
                    String s = "";
                    for (int j=0; j<12; j++) {
                        s += windowWeek.getTimeframes()[i][j] + " ";

                    }
                    System.out.println(s);
                }

                long[] total = windowWeek.getEstimatedTotal();

                if(this.status == 6){
                    String result = "";
                    for (int i = 0; i < total.length; i++){
                        result += total[i] + " ";
                    }
                    System.err.println("RESULT: " + result);
                    producer.send(new ProducerRecord<>(TOPIC_2_OUTPUT, result));
                    this.status = 0;

                }

                this.status++;

                // Avanzo la finestra
                //this.windowWeek = new SlotBasedWindowWeek();
                this.windowWeek.moveForward(elapsedDay);

                // Aggiorno il timestamp
                this.lastTick = tupleTimestamp;

            }
        }

        else {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            windowWeek.updateSlot(tupleTimestamp);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
