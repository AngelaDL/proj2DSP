package main.java.config;

public class Configuration {

    // Kafka
    private static final String KAFKA_BROKER_0 = "localhost:9092";
    private static final String KAFKA_BROKER_1 = "localhost:9093";
    private static final String KAFKA_BROKER_2 = "localhost:9094";
    private static final String SEP = ",";

    public static final String ZOOKEEPER = "localhost:2181";
    public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_0;
    public static final String BROKER_SERVERS =
            KAFKA_BROKER_0 + SEP +
                    KAFKA_BROKER_1 + SEP +
                    KAFKA_BROKER_2;

    public static String KAFKA_PORT = "localhost:9092";
    public static final String TOPIC_1_INPUT = "query-1-input";
    public static final String TOPIC_2_INPUT = "query-2-input";
    public static final String TOPIC_3_INPUT = "query-3-input";
    public static final String TOPIC_1_OUTPUT = "query-1-output";
    public static final String TOPIC_2_OUTPUT = "query-2-output";
    public static final String TOPIC_3_OUTPUT = "query-3-output";
    public static final String PRODUCER_GROUPID = "producer";
    public static final String CONSUMER_GROUPID = "consumer";
    public static final String GLOBAL_GROUP_ID = "nyt-comments";

    // Dataset configuration
    public static final String DATASET = "/home/angela/IdeaProjects/proj2DSP/dataset/Comments_jan-apr2018.csv";

    // Discard probability for the SamplingBolt
    public static final double PERCENT = 0.005;

    // Data fields
    public static final String RAW_DATA = "rawdata";
    public static final String CURRENT_TIMESTAMP = "current";
    public static final String CREATE_DATE = "create_date";
    public static final String ARTICLE_ID = "article_id";
    public static final String METRONOME_H_STREAM_ID = "h_msg";
    public static final String METRONOME_D_STREAM_ID = "d_msg";
    public static final String PARSER_STREAM_ID = "parser";
    public static final String ESTIMATED_TOTAL = "estimated_total";
    public static final String PARTIAL_RANKING = "partial_ranking";
    public static final String COMMENT_TYPE = "comment_type";
    public static final String USER_ID = "user_id";
    public static final String COMMENT_ID = "comment_id";
    public static final String DEPTH = "depth";
    public static final String EDITOR_SELECTION = "editor_selection";
    public static final String IN_REPLY_TO = "in_reply_to";
    public static final String PARENT_USER_NAME = "parent_user_name";
    public static final String RECCOMENDATIONS = "reccomendations";
    public static final String H_ID = "h_id";
    public static final String D_ID = "d_id";
    public static final String W_ID = "w_id";
    public static final String TIME_ID = "time_id";
}
