package main.java.queries;

import main.java.operators.MetronomeBolt;
import main.java.operators.SamplingBolt;
import main.java.operators.query1.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static main.java.config.Configuration.*;

public class TopologyQ1 {

    private static final String SPOUT = "spout";
    private static final String PARSER = "parser";
    private static final String SAMPLING = "sampling";
    private static final String METRONOME = "metronome";
    private static final String COUNT_BY_H = "countByH";
    private static final String COUNT_BY_D = "countByD";
    private static final String PARTIAL = "partial";
    private static final String GLOBAL = "global";


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT, new KafkaSpout(), 1);

        builder.setBolt(PARSER, new ParserBolt(), 1)
                .localOrShuffleGrouping(SPOUT);

        builder.setBolt(SAMPLING, new SamplingBolt(), 1)
                .allGrouping(PARSER);

        builder.setBolt(METRONOME, new MetronomeBolt(), 1)
                .allGrouping(SAMPLING);

        builder.setBolt(COUNT_BY_D, new CountByDayBolt(), 1)
                .allGrouping(PARSER)
                .allGrouping(METRONOME, METRONOME_D_STREAM_ID);

        builder.setBolt(PARTIAL, new PartialRankBolt(3), 1)
                .allGrouping(COUNT_BY_D);

        builder.setBolt(GLOBAL, new GlobalRankBolt(true, 3, TOPIC_1_OUTPUT), 1)
                .allGrouping(PARTIAL);

        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        try {
            KAFKA_PORT = "localhost:9092";
            int stormWorkers = 3;
            conf.setNumWorkers(stormWorkers);
        } catch (Exception e) {
            System.err.println("You have to specify kafka host and the number of the workers");
            e.printStackTrace();
        }

        localCluster.submitTopology("query1", conf, builder.createTopology());

    }
}
