package main.java.operators.query1;

import main.java.utils.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static config.Configuration.*;

public class CountByHourBolt extends BaseRichBolt {
    private Map<String, Window> map;
    private OutputCollector _collector;
    private long lastTick;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.map = new HashMap<String, Window>();
        this.lastTick = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        String msgType = tuple.getSourceStreamId();

        // When a tick by metronome is received, it handles the window shifting operations
        if (msgType.equals(METRONOME_H_STREAM_ID)) {
            long tupleTimestamp = tuple.getLongByField(TIMESTAMP);
            long currentTimestamp = tuple.getLongByField(CURRENNT_TIMESTAMP);
            int elapsedHour = (int) Math.ceil((tupleTimestamp - lastTick) / MetronomeBolt.MILLIS_H);
            System.err.println("ELAPSED: " + elapsedHour);

            // Control: only informations relating to the current window are processed
            for (String articleID : this.map.keySet()) {
                Window window = this.map.get(articleID);

                Values values = new Values();
                values.add(tupleTimestamp);
                values.add(currentTimestamp);
                values.add(METRONOME_H_STREAM_ID);
                values.add(articleID);
                values.add(window.getEstimatedTotal());

                _collector.emit(values);

                window.moveForward(elapsedHour);
            }
            this.lastTick = tupleTimestamp;
        }

        // When a msg from parser is received, it handles memorization operations in the window
        else {
            String articleID = tuple.getStringByField(ARTICLE_ID);
            long timestamp = tuple.getLongByField(CREATE_DATE);

            System.out.println("COUNT ARTICLE_ID: " + articleID);
            System.out.println("COUNT TIMESTAMP: " + timestamp);

            // Control: only informations relating to the current window are processed
            if (timestamp > this.lastTick) {
                // If there isn't the key in the map, create a new <key, value> object
                Window window = this.map.get(articleID);
                if (window == null) {
                    window = new Window(1);
                    map.put(articleID, window); // This is a tumbling window
                }

                System.out.println("WINDOW: " + window.getEstimatedTotal());
                window.increment();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENNT_TIMESTAMP, METRONOME_H_STREAM_ID, ARTICLE_ID, ESTIMATED_TOTAL));
    }
}
