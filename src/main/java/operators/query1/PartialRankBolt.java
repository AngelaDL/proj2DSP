package main.java.operators.query1;

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

import static config.Configuration.*;

public class PartialRankBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private TopKRanking topKranking;
    private int k;

    public PartialRankBolt(int k) {
        this.k = k;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this.topKranking = new TopKRanking(this.k);
    }

    @Override
    public void execute(Tuple tuple) {
        long tupleTimestamp = tuple.getLongByField(CREATE_DATE);
        long currentTimestamp = tuple.getLongByField(CURRENNT_TIMESTAMP);
        String metronomeMsg = tuple.getStringByField(METRONOME_H_STREAM_ID);
        String articleID = tuple.getStringByField(ARTICLE_ID);
        long estimatedTotal = tuple.getLongByField(ESTIMATED_TOTAL);

        System.out.println("PARTIAL RANK BOLT: " + tupleTimestamp + " " + currentTimestamp);
        boolean update = false;
        RankItem item = new RankItem(articleID, estimatedTotal);
        update = topKranking.update(item);

        if (update) {
            Ranking ranking = topKranking.getTopK();

            Values values = new Values(tupleTimestamp, currentTimestamp, ranking, metronomeMsg);
            //values.add(tupleTimestamp);
            //values.add(currentTimestamp);
            //values.add(gson.toJson(ranking));
            //values.add(ranking);
            //values.add(metronomeMsg);

            System.err.println("PARTIAL RANK VALUES: " + values);

            _collector.emit(values);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(CREATE_DATE, CURRENNT_TIMESTAMP, PARTIAL_RANKING, METRONOME_H_STREAM_ID));
    }
}
