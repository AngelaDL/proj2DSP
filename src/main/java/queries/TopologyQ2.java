package main.java.queries;

import main.java.operators.MetronomeBolt;
import main.java.operators.SamplingBolt;
import main.java.operators.query2.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static main.java.config.Configuration.*;

public class TopologyQ2 {

    private static final String SPOUT2 = "spout2";
    private static final String PARSER2 = "parser2";
    private static final String COUNT_BY_DAY = "count_by_day";
    private static final String COUNT_BY_WEEK = "count_by_week";
    private static final String COUNT_BY_MONTH = "count_by_month";
    private static final String SAMPLING = "sampling";
    private static final String METRONOME = "metronome";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT2, new KafkaSpout2(), 1);

        builder.setBolt(PARSER2, new ParserBolt2(), 1)
                .shuffleGrouping(SPOUT2);

        builder.setBolt(SAMPLING, new SamplingBolt(), 1)
                .allGrouping(PARSER2);

        builder.setBolt(METRONOME, new MetronomeBolt(), 1)
                .allGrouping(SAMPLING);

        builder.setBolt(COUNT_BY_DAY, new CountByDay(), 1)
                .shuffleGrouping(PARSER2)
                .allGrouping(METRONOME, METRONOME_D_STREAM_ID);

        builder.setBolt(COUNT_BY_WEEK, new CountByWeek(), 1)
                .shuffleGrouping(PARSER2)
                .allGrouping(METRONOME, METRONOME_D_STREAM_ID);

        builder.setBolt(COUNT_BY_MONTH, new CountByMonth(), 1)
                .shuffleGrouping(PARSER2)
                .allGrouping(METRONOME, METRONOME_D_STREAM_ID);

        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        try {
            KAFKA_PORT = "localhost:9092";
            conf.setNumWorkers(3);
        } catch (Exception e) {
            e.printStackTrace();
        }

        localCluster.submitTopology("query2", conf, builder.createTopology());
    }
}
