package main.java.operators.query2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static main.java.config.Configuration.*;

public class ParserBolt2 extends BaseRichBolt {
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    /*
     * Data Format:
     *    0    approveDate
     *    1    articleID
     *    2    articleWordCount
     *    3    commentID
     *    4    commentType             x
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

        String[] splitted = rawdata.split(",");

        String create_date = splitted[5];
        String comment_type = splitted[4];


        if(comment_type.equals("comment")) {
            //System.out.println("COMMENT TYPE: " + comment_type);
            Values values = new Values(Long.parseLong(create_date)*1000, comment_type, currentTimestamp);
            //System.out.println("VALUES QUERY 2: " + values);
            _collector.emit(values);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, COMMENT_TYPE, CURRENT_TIMESTAMP));
    }
}
