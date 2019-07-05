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

public class MetronomeBolt extends BaseRichBolt {

    public static final long MILLIS_H = 1000*60*60;
    public static final long MILLIS_D = 1000*60*60*24;

    private OutputCollector _collector;
    private long elapsedTime_h;
    private long elapsedTime_d;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.elapsedTime_h = 0;
        this.elapsedTime_d = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        long time = tuple.getLongByField(CREATE_DATE);
        long currentTimestamp = tuple.getLongByField(CURRENNT_TIMESTAMP);

        if (this.elapsedTime_h == 0)
            this.elapsedTime_h = time;
        if (this.elapsedTime_d == 0)
            this.elapsedTime_d = time;

        else {
            // Metronome sends tick every hour
            if (time - this.elapsedTime_h >= MILLIS_H) {
                this.elapsedTime_h = 0;
                Values values = new Values();
                values.add(time);
                values.add(currentTimestamp);
                _collector.emit(METRONOME_H_STREAM_ID, values);
            }

            // Metronome sends tick every day
            if (time - this.elapsedTime_d >= MILLIS_D) {
                this.elapsedTime_d = 0;
                Values values = new Values();
                values.add(time);
                values.add(currentTimestamp);
                _collector.emit(METRONOME_D_STREAM_ID, values);
            }
        }

        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //TODO controllare bene...forse bisogna usare declareStream??
        outputFieldsDeclarer.declareStream(METRONOME_H_STREAM_ID, new Fields(CREATE_DATE, CURRENNT_TIMESTAMP));
        outputFieldsDeclarer.declareStream(METRONOME_D_STREAM_ID, new Fields(CREATE_DATE, CURRENNT_TIMESTAMP));
    }
}
