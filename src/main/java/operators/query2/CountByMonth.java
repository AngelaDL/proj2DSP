package main.java.operators.query2;

import main.java.operators.MetronomeBolt;
import main.java.utils.SlotBasedWindowMonth;
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

public class CountByMonth extends BaseRichBolt {

    private OutputCollector _collector;
    private SlotBasedWindowMonth windowMonth;
    private long lastTick;
    private KafkaProducer<String, String> producer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.windowMonth = new SlotBasedWindowMonth();
        this.lastTick = 0;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_PORT);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void execute(Tuple tuple) {
        String msgType = tuple.getSourceStreamId();

        if(msgType.equals(METRONOME_D_STREAM_ID)) {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            long timestamp = tuple.getLongByField(CURRENT_TIMESTAMP);

            if(tupleTimestamp > this.lastTick) {
                int elapsedDay = (int) Math.ceil((tupleTimestamp - lastTick) / MetronomeBolt.MILLIS_D);

                System.out.println("window 30 x 12");
                for (int i = 0; i < 30; i++) {
                    String s = "";
                    for (int j=0; j<12; j++) {
                        s += windowMonth.getTimeframes()[i][j] + " ";

                    }
                    System.out.println(s);
                }


                long[] total = windowMonth.getEstimatedTotal();

                String result = "";
                for (int i = 0; i < total.length; i++){
                    result += total[i] + " ";
                }
                System.err.println("Result: " + result);
                producer.send(new ProducerRecord<>(TOPIC_2_OUTPUT, result));

                // Avanzo la finestra
                this.windowMonth.moveForward(elapsedDay);

                // Aggiorno il timestamp
                this.lastTick = tupleTimestamp;

            }
        }

        else {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            windowMonth.updateSlot(tupleTimestamp);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
