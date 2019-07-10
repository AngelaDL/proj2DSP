package main.java.operators.query1;

import main.java.utils.DateUtils;
import main.java.utils.RankItem;
import main.java.utils.Ranking;
import main.java.utils.TopKRanking;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static main.java.config.Configuration.*;

public class PartialRankBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private TopKRanking ranking;
    private int topK;

    public PartialRankBolt(int k) {
        this.topK = k;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes")Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.ranking = new TopKRanking(topK);
    }

    @Override
    public void execute(Tuple tuple) {
        String msgType = tuple.getStringByField(TIME_ID);
        String articleID = tuple.getStringByField(ARTICLE_ID);
        long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
        Long currentTimestamp = tuple.getLongByField(CURRENT_TIMESTAMP);
        long estimatedTotal = tuple.getLongByField(ESTIMATED_TOTAL);

        //System.out.println("CREATE_DATE_PARTIAL: " + DateUtils.getDate(tupleTimestamp));
        //boolean update = ranking;
        RankItem item = new RankItem(articleID, estimatedTotal);
        //System.out.println("RANK ITEM: " + DateUtils.getDate(tupleTimestamp) + " " + item);
        boolean updated = ranking.update(item);

        if (updated) {
            Ranking topK = ranking.getTopK();
            //System.out.println("TOPK: " + String.valueOf(topK));
            Values values = new Values();
            values.add(tupleTimestamp);
            values.add(currentTimestamp);
            values.add(topK);
            values.add(msgType);
            //System.err.println("PARTIAL RANK VALUES: " + DateUtils.getDate(tupleTimestamp) + values);
            _collector.emit(values);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENT_TIMESTAMP, PARTIAL_RANKING, TIME_ID));
    }
}
