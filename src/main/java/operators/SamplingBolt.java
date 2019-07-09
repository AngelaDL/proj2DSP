package main.java.operators;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static main.java.config.Configuration.*;

public class SamplingBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
            long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
            long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
            boolean b = false;

            double p = Math.random();
            if (p > PERCENT) {
                b = true;
            }

            if(b) {
                Values values = new Values();
                values.add(tupleTimestamp);
                values.add(currentTimestamp);
                _collector.emit(values);
                _collector.ack(tuple);
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENT_TIMESTAMP));
    }

    /*private boolean checkPercentage() {
        double p = Math.random();
        if (p > PERCENT)
            return true;
        return false;
    }*/
}
