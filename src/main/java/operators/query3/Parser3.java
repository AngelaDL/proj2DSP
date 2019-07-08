package main.java.operators.query3;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import scala.Int;

import java.util.Map;

import static main.java.config.Configuration.*;

public class Parser3 extends BaseRichBolt {
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
     *    3    commentID               x
     *    4    commentType             x
     *    5    createDate              x
     *    6    depth                   x
     *    7    editorSelection         x
     *    8    inReplyTo               x
     *    9    parentUserDisplayName   x
     *   10    recommendations         x
     *   11    sectionName
     *   12    userDisplayName
     *   13    userID                  x
     *   14    userLocation
     */

    @Override
    public void execute(Tuple tuple) {
        String rawdata = tuple.getStringByField(RAW_DATA);
        long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);

        String[] splitted = rawdata.split(",");

        int comment_id = Integer.parseInt(splitted[3]);
        String comment_type = splitted[4];
        long create_date = Long.parseLong(splitted[5]);
        //int depth = Integer.parseInt(splitted[6]);
        boolean editor_selection = Boolean.parseBoolean(splitted[7]);
        String in_reply_to = splitted[8];
        //String parentUserDisplayName = splitted[9];
        String reccomendations = splitted[10];
        String userID = splitted[13];

        if(isInteger(reccomendations)){
            Values values = new Values(create_date*1000, currentTimestamp, userID, comment_id, comment_type, editor_selection,
                    in_reply_to, Integer.parseInt(reccomendations));
            _collector.emit(values);
            System.out.println(values);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENT_TIMESTAMP, USER_ID, COMMENT_ID, COMMENT_TYPE, EDITOR_SELECTION,
                IN_REPLY_TO, RECCOMENDATIONS));

    }

    public boolean isInteger(String x){
        try {
            Integer.parseInt(x);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
