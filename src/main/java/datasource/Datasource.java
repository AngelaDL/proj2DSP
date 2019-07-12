package main.java.datasource;

import main.java.kafka.SimpleKakfaProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.TimeZone;

import static main.java.config.Configuration.DATASET;
import static main.java.config.Configuration.TOPIC_1_INPUT;

public class Datasource implements Runnable {

    private static final int TIMESPAN = 1; 		// expressed in mins
    private static final int SPEEDUP = 1000; 	// expressed in ms

    private SimpleKakfaProducer producer;

    public Datasource() {

        this.producer =  new SimpleKakfaProducer(TOPIC_1_INPUT);

    }


    @Override
    public void run() {
        BufferedReader br = null;

        try {
            System.out.println("Initializing... ");
            br = new BufferedReader(new FileReader(DATASET));
            String header = br.readLine();
            System.out.println("HEADER: " + header);

            String line = br.readLine();
            long previousTime = getEventTime(line);
            long latestSendingTime = System.currentTimeMillis();
            producer.produce(null, line);

            while ((line = br.readLine()) != null) {

                long nextTime = getEventTime(line);
                long sleepTime = (int) Math.floor(((double) (nextTime - previousTime ) / (TIMESPAN * 60 * 1000)));
                long deltaIntervalToSkip = SPEEDUP - (System.currentTimeMillis() - latestSendingTime);
                if(sleepTime > 0) {

                    //System.out.println(" sleep for :" + sleepTime + " ms");

                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                producer.produce(null, line);
                latestSendingTime = System.currentTimeMillis();
                previousTime = nextTime;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (br != null){
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static long getEventTime(String line) {

        String[] tokens	= line.split(",");
        long ts = Long.valueOf(tokens[5])*1000;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date d = new Date(ts);
        return d.getTime();

    }

    public static void main(String[] args) {

        Datasource fill = new Datasource();
        Thread th1 = new Thread(fill);
        th1.start();
    }


}
