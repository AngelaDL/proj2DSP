package main.java.operators.query2;

import main.java.config.Configuration;
import main.java.operators.MetronomeBolt;
import main.java.utils.SlotBasedWindow;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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

public class CountByDay extends BaseRichBolt {

    private OutputCollector _collector;
    private SlotBasedWindow window;
    private int current;
    private long lastTick;
    private KafkaProducer<String, String> producer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.window = new SlotBasedWindow();
        this.current = 0;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_IP_PORT);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<String, String>(properties);

    }

    @Override
    public void execute(Tuple tuple) {

        String msgType = tuple.getSourceStreamId();

        // When a tick by metronome is received, it handles the window shifting operations
        if (msgType.equals(METRONOME_D_STREAM_ID)) {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            long timestamp = tuple.getLongByField(CURRENT_TIMESTAMP);


            //quando mi arriva il tick di un giorno dal metronomo, posso produrre i risultati
            if(tupleTimestamp > this.lastTick) {
                //calcolo quanto tempo è trascorso
                int elapsedHour = (int) Math.ceil((tupleTimestamp - lastTick) / MetronomeBolt.MILLIS_D);
                long[] windowSize = window.getTimeframes();

                String window = "";
                for(int i = 0; i < windowSize.length; i ++) {
                    window += windowSize[i] + " ";
                }
                System.out.println("RESULT: " + window);
                producer.send(new ProducerRecord<String, String>(TOPIC_2_OUTPUT, window));

                // Avanzo la finestra
                this.window.moveForward(elapsedHour);
                // Aggiorno il timestamp
                this.lastTick = tupleTimestamp;
            }
        }

        // When a msg from parser is received, it handles memorization operations in the window
        else {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            window.updateSlot(tupleTimestamp);

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
