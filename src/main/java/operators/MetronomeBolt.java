package main.java.operators;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import static main.java.config.Configuration.*;

public class MetronomeBolt extends BaseRichBolt {

    public static final long MILLIS_H = 1000*60*60;
    public static final long MILLIS_D = 1000*60*60*24;

    private OutputCollector _collector;
    private long time_h;
    private long time_d;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.time_h = 0;
        this.time_d = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        long time = tuple.getLongByField(CREATE_DATE);
        long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);

        // Metronome sends tick every hour
        if (this.time_h < time && (time - this.time_h) >= MILLIS_H) {
            this.time_h = roundToCompletedHour(time);
            Values values = new Values();
            values.add(time);
            values.add(currentTimestamp);
            //System.out.println("E' PASSATA UN ORA");
            _collector.emit(METRONOME_H_STREAM_ID, values);
        }

        // Metronome sends tick every day
        if (this.time_d < time && (time - this.time_d) >= MILLIS_D) {
            this.time_d = roundToCompletedDay(time);
            Values values = new Values();
            values.add(time);
            values.add(currentTimestamp);
            System.out.println("E' PASSATO UN GIORNO");
            _collector.emit(METRONOME_D_STREAM_ID, values);
        }

        _collector.ack(tuple);
    }


    private long roundToCompletedHour(Long timestamp) {

        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        date.set(Calendar.MINUTE, 0);

        return date.getTime().getTime();
    }


    private long roundToCompletedDay(long timestamp) {

        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.HOUR, 0);

        return date.getTime().getTime();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //TODO controllare bene...forse bisogna usare declareStream??
        outputFieldsDeclarer.declareStream(METRONOME_H_STREAM_ID, new Fields(CREATE_DATE, CURRENT_TIMESTAMP));
        outputFieldsDeclarer.declareStream(METRONOME_D_STREAM_ID, new Fields(CREATE_DATE, CURRENT_TIMESTAMP));
    }
}
