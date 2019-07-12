package main.java.utils;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class Test {
    public static void main(String[] args) throws IOException {
        //int timestamp = 1514956474;
        String result = "Ciao";
        String filename = "prova";

        FileWriter fw = new FileWriter();
        fw.writeResult(filename, result);

        //int day_of_week = getDay(timestamp);
        //System.out.println("DAY: " + day_of_week);

        //int day_of_month = getDayOfMonth(timestamp);
        //System.out.println("DAY: " + day_of_month);



    }

    public static int getDay(long timestamp) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        calendar.setTimeInMillis(timestamp*1000);
        System.out.println("Current time is: " + calendar.getTime());

        int day = calendar.get(Calendar.DAY_OF_WEEK);
        int result = -1;

        switch (day) {
            case Calendar.MONDAY:
                result = 0;
                break;
            case Calendar.TUESDAY:
                result = 1;
                break;
            case Calendar.WEDNESDAY:
                result = 2;
                break;
            case Calendar.THURSDAY:
                result = 3;
                break;
            case Calendar.FRIDAY:
                result = 4;
                break;
            case Calendar.SATURDAY:
                result = 5;
                break;
            case Calendar.SUNDAY:
                result = 6;
                break;
        }
        return result;
    }

    public static int getDayOfMonth(long timestamp) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        calendar.setTimeInMillis(timestamp*1000);
        System.out.println("Current time is: " + calendar.getTime());
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        //System.out.println("HOUR: " + hour);

        return day;
    }
}
