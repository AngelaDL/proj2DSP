package main.java.operators.query1;

import main.java.utils.ValidityControl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static main.java.config.Configuration.*;

public class ParserBolt extends BaseRichBolt {

    private OutputCollector _collector;
    //private SimpleDateFormat sdf;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //this.sdf = new SimpleDateFormat();
        this._collector = outputCollector;
    }

    /*
     * Data Format:
     *    0    approveDate
     *    1    articleID               x
     *    2    articleWordCount
     *    3    commentID
     *    4    commentType
     *    5    createDate              x
     *    6    depth
     *    7    editorSelection
     *    8    inReplyTo
     *    9    parentUserDisplayName
     *   10    recommendations
     *   11    sectionName
     *   12    userDisplayName
     *   13    userID
     *   14    userLocation
     */
    @Override
    public void execute(Tuple tuple) {

        String rawdata = tuple.getStringByField(RAW_DATA);
        long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
        //System.out.println("RAWDATA: " + rawdata);
        //System.out.println("TIMESTAMP: " + currentTimestamp);
        String[] splitted = rawdata.split(",");
        //for(int i = 0; i<splitted.length; i++)
        //    System.out.println("SPLITTED: " + i + " " + splitted[i]);

        String timestamp = splitted[5];
        String article_id = splitted[1];
        //System.err.println("TIMESTAMP: " + timestamp);
        //if (validateTuple(splitted)) {

        Values values = new Values(Long.parseLong(timestamp), article_id, currentTimestamp);
        //values.add(splitted[1]);
        //values.add(splitted[5]);
        //values.add(currentTimestamp);

        System.out.println("PARSER VALUES: " +  values);

        _collector.emit(values);
        //}
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, ARTICLE_ID, CURRENT_TIMESTAMP));
    }

    private boolean validateTuple(String[] fields) {
        if (fields.length < 15)
            return false;

        if (!ValidityControl.timestamp(fields[0]) || !ValidityControl.timestamp(fields[5]))
            return false;

        if (!ValidityControl.unsignedInteger(fields[3]) ||
            !ValidityControl.unsignedInteger(fields[2]) ||
            !ValidityControl.unsignedInteger(fields[13]))
            return false;

        if (!ValidityControl.commentType(fields[4]))
            return false;

        if (!ValidityControl.depth(fields[6]))
            return false;

        if (!ValidityControl.isBool(fields[7]))
            return false;

        if (!ValidityControl.reply(fields[8]))
            return false;

        if (!ValidityControl.isInteger(fields[10]))
            return false;

        if (!ValidityControl.isNullOrEmpty(fields[1]) ||
                !ValidityControl.isNullOrEmpty(fields[9]) ||
                !ValidityControl.isNullOrEmpty(fields[11]) ||
                !ValidityControl.isNullOrEmpty(fields[12]) ||
                !ValidityControl.isNullOrEmpty(fields[14]))
            return false;

        return true;
    }
}
