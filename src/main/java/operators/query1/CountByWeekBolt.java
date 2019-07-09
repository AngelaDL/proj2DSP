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

import static main.java.config.Configuration.*;

public class CountByWeekBolt extends BaseRichBolt {

    private OutputCollector _collector;
    private Map<String, Window> map;
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

        if(msgType.equals(METRONOME_D_STREAM_ID)) {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            Long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);

            if(tupleTimestamp > this.lastTick) {
                int elapsedDay = (int) Math.ceil((tupleTimestamp - currentTimestamp) / (1000*60*60*24));

                for (String articleID : map.keySet()) {
                    Window window = map.get(articleID);
                    long estimatedTotal = window.getEstimatedTotal();

                    Values values = new Values(tupleTimestamp, articleID, estimatedTotal, W_ID, currentTimestamp);
                    _collector.emit(values);

                    window.moveForward(elapsedDay);
                }
                this.lastTick = tupleTimestamp;
            }

        }
        else {
            String articleID = tuple.getStringByField(ARTICLE_ID);
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);

            if(tupleTimestamp >= this.lastTick) {
                Window window = map.get(articleID);
                if(window == null) {
                    window = new Window(7);
                    map.put(articleID, window);
                }
                window.increment();
            }
        }
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, ARTICLE_ID, ESTIMATED_TOTAL, TIME_ID, CURRENT_TIMESTAMP));
    }
}
