package main.java.queries;

import main.java.operators.query1.KafkaSpout;
import main.java.operators.query2.KafkaSpout2;
import main.java.operators.query2.ParserBolt2;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static main.java.config.Configuration.*;

public class TopologyQ2 {

    private static final String SPOUT2 = "spout2";
    private static final String PARSER2 = "parser2";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT2, new KafkaSpout2(), 1);
        builder.setBolt(PARSER2, new ParserBolt2(), 1)
                .shuffleGrouping(SPOUT2);

        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        try {
            KAFKA_IP_PORT = "localhost:9092";
            conf.setNumWorkers(3);
        } catch (Exception e) {
            System.err.println("You have to specify kafka host and the number of the workers");
            e.printStackTrace();
        }

        localCluster.submitTopology("query2", conf, builder.createTopology());
    }
}
