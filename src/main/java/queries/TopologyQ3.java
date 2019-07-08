package main.java.queries;

import main.java.operators.query3.KafkaSpout3;
import main.java.operators.query3.Parser3;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import static main.java.config.Configuration.KAFKA_PORT;

public class TopologyQ3 {
    private static final String SPOUT3 = "spout3";
    private static final String PARSER3 = "parser3";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT3, new KafkaSpout3(), 1);

        builder.setBolt(PARSER3, new Parser3(), 1)
                .shuffleGrouping(SPOUT3);

        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        try {
            KAFKA_PORT = "localhost:9092";
            conf.setNumWorkers(3);
        } catch (Exception e) {
            System.err.println("You have to specify kafka host and the number of the workers");
            e.printStackTrace();
        }

        localCluster.submitTopology("query3", conf, builder.createTopology());
    }
}
