package main.java.operators.query1;

import main.java.utils.DateUtils;
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

import static main.java.config.Configuration.*;

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

            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);

            if(tupleTimestamp > this.lastTick) {
                int elapsedHour = (int) Math.ceil((tupleTimestamp - lastTick) / (1000*60*60));

                // Control: only informations relating to the current window are processed
                for (String articleID : map.keySet()) {
                    Window window = map.get(articleID);
                    long estimatedTotal = window.getEstimatedTotal();

                    Values values = new Values();
                    values.add(tupleTimestamp);
                    values.add(currentTimestamp);
                    values.add(H_ID);
                    values.add(articleID);
                    values.add(estimatedTotal);
                    //System.out.println("COUNT BY H: " + DateUtils.getDate(tupleTimestamp) + " " + values);

                    _collector.emit(values);

                    window.moveForward(elapsedHour);
                }
                this.lastTick = tupleTimestamp;
            }

        }

        // When a msg from parser is received, it handles memorization operations in the window
        else {
            String articleID = tuple.getStringByField(ARTICLE_ID);
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);

            // Control: only informations relating to the current window are processed
            if (tupleTimestamp > this.lastTick) {
                // If there isn't the key in the map, create a new <key, value> object
                Window window = map.get(articleID);
                if (window == null) {
                    window = new Window(1);
                    map.put(articleID, window);
                }

                //System.out.println("WINDOW: " + window.getEstimatedTotal());
                window.increment();
            }
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENT_TIMESTAMP, TIME_ID, ARTICLE_ID, ESTIMATED_TOTAL));
    }
}
