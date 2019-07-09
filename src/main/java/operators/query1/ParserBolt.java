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

        String tupleTimestamp = splitted[5];
        String comment_type = splitted[4];
        String article_id = splitted[1];

        if(comment_type != null) {
            Values values = new Values(Long.parseLong(tupleTimestamp) * 1000, article_id, currentTimestamp);
            _collector.emit(values);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, ARTICLE_ID, CURRENT_TIMESTAMP));
    }
}
