package main.java.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateUtils {


    public static int getSlot(long timestamp) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        calendar.setTimeInMillis(timestamp);
        //System.out.println("Current time is: " + calendar.getTime());
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        //System.out.println("HOUR: " + hour);

        return hour/2;
    }

    public static int getDay(long timestamp) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        calendar.setTimeInMillis(timestamp);

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
        calendar.setTimeInMillis(timestamp);
        //System.out.println("Current time is: " + calendar.getTime());
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        //System.out.println("HOUR: " + hour);

        return day;
    }

    public static Date getDate(long timestamp) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        calendar.setTimeInMillis(timestamp);
        //System.out.println("Current time is: " + calendar.getTime());
        Date date = calendar.getTime();
        //System.out.println("HOUR: " + hour);

        return date;
    }
}
