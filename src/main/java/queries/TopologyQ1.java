package main.java.queries;

import main.java.operators.MetronomeBolt;
import main.java.operators.SamplingBolt;
import main.java.operators.query1.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static main.java.config.Configuration.*;

public class TopologyQ1 {

    private static final String SPOUT = "spout";
    private static final String PARSER = "parser";
    private static final String SAMPLING = "sampling";
    private static final String METRONOME = "metronome";
    private static final String COUNT_BY_H = "countByH";
    private static final String COUNT_BY_D = "countByD";
    private static final String COUNT_BY_W = "countByW";
    private static final String PARTIAL_H = "partial_h";
    private static final String PARTIAL_D = "partial_d";
    private static final String PARTIAL_W = "partial_w";
    private static final String GLOBAL_H = "global_h";
    private static final String GLOBAL_D = "global_d";
    private static final String GLOBAL_W = "global_w";


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT, new KafkaSpout(), 1);

        builder.setBolt(PARSER, new ParserBolt(), 1)
                .shuffleGrouping(SPOUT);

        builder.setBolt(SAMPLING, new SamplingBolt(), 1)
                .setNumTasks(1)
                .allGrouping(PARSER);

        builder.setBolt(METRONOME, new MetronomeBolt(), 1)
                .setNumTasks(1)
                .allGrouping(SAMPLING);

        /*builder.setBolt(COUNT_BY_H, new CountByHourBolt(), 1)
                .fieldsGrouping(PARSER, new Fields(ARTICLE_ID))
                .allGrouping(METRONOME, METRONOME_H_STREAM_ID);

        builder.setBolt(PARTIAL_H, new PartialRankBolt(3), 1)
                .fieldsGrouping(COUNT_BY_H, new Fields(ARTICLE_ID));

        builder.setBolt(GLOBAL_H, new GlobalRankBolt(3), 1)
                .setNumTasks(1)
                .allGrouping(PARTIAL_H); */

        builder.setBolt(COUNT_BY_D, new CountByDayBolt(), 1)
                .allGrouping(PARSER)
                .allGrouping(METRONOME, METRONOME_D_STREAM_ID);

        builder.setBolt(PARTIAL_D, new PartialRankBolt(3), 1)
                .fieldsGrouping(COUNT_BY_D, new Fields(ARTICLE_ID));

        builder.setBolt(GLOBAL_D, new GlobalRankBolt(3))
                .allGrouping(PARTIAL_D);

        /* builder.setBolt(COUNT_BY_W, new CountByWeekBolt(), 1)
                .fieldsGrouping(PARSER, new Fields(ARTICLE_ID))
                .allGrouping(METRONOME, METRONOME_D_STREAM_ID);

        builder.setBolt(PARTIAL_W, new PartialRankBolt(3), 1)
                .fieldsGrouping(COUNT_BY_W, new Fields(ARTICLE_ID));

        builder.setBolt(GLOBAL_W, new GlobalRankBolt(3))
                .allGrouping(PARTIAL_W);*/

        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        try {
            KAFKA_PORT = "localhost:9092";
            conf.setNumWorkers(3);
        } catch (Exception e) {
            e.printStackTrace();
        }

        localCluster.submitTopology("query1", conf, builder.createTopology());

    }
}
