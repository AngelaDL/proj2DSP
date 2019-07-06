package main.java.operators.query2;

import main.java.utils.SlotBasedWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static main.java.config.Configuration.*;

public class CountByDay extends BaseRichBolt {

    private OutputCollector _collector;
    private SlotBasedWindow window;
    private int current;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.window = new SlotBasedWindow();
        this.current = 0;

    }

    @Override
    public void execute(Tuple tuple) {

        String msgType = tuple.getSourceStreamId();

        // When a tick by metronome is received, it handles the window shifting operations
        if (msgType.equals(METRONOME_D_STREAM_ID)) {
            long tupleTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);

        }

        // When a msg from parser is received, it handles memorization operations in the window
        else {

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
