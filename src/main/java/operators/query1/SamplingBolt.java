package main.java.operators.query1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static config.Configuration.*;

public class SamplingBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (checkPercentage()) {
            //String timestamp = tuple.getStringByField(CREATE_DATE);
            //System.out.println("SAMPLING TIMESTAMP: " + timestamp);
            long timestamp = tuple.getLongByField(CREATE_DATE);
            long originalTupleTimestamp = tuple.getLongByField(CURRENNT_TIMESTAMP);

            Values values = new Values();
            values.add(timestamp);
            values.add(originalTupleTimestamp);
            _collector.emit(values);
            _collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENNT_TIMESTAMP));
    }

    private boolean checkPercentage() {
        double p = Math.random();
        if (p < PERCENT/100)
            return false;
        return true;
    }
}
