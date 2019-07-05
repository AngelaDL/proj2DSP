package datasource;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import config.Configuration;
import main.java.kafka.*;

public class Datasource {

    public static void main(String[] args) {

        SimpleKakfaProducer producer = new SimpleKakfaProducer(Configuration.TOPIC_1_INPUT);

        BufferedReader br = null;
        String line = "";

        try {

            br = new BufferedReader(new FileReader(Configuration.DATASET));

            String header = br.readLine();
            String firstLine = br.readLine();
            long eventTime = getEventTime(firstLine);

            producer.produce(null, firstLine);

            int k = 0;
            while ((line = br.readLine()) != null && k<3000) {
                {
                    long actualEventTime = getEventTime(line);
                    long diff = (actualEventTime - eventTime);// /1000
                    Thread.sleep(diff);
                    eventTime = actualEventTime;
                    producer.produce(null, line);
                    k++;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static long getEventTime(String line) {
        long ts = 0;
        String[] tokens = line.split(",");
        ts = Long.parseLong(tokens[5]);
        return ts;

    }

}