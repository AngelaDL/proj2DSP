package main.java.utils;

import java.time.Instant;
import java.util.Date;

/**
 * The methods in this class are used to validate the fields passed by datasource
 */
public class ValidityControl {

    // Used for fields 0, 5
    public static boolean timestamp(String timestamp) {
        try {
            Date.from(Instant.ofEpochSecond(Long.parseLong(timestamp)));
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }


    // Used for fields 3, 2, 13
    public static boolean unsignedInteger(String number) {
        try {
            int n = Integer.parseInt(number);
            if ((n < 0) || (n > 4294967295L))
                return false;
            else
                return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Used for field 4
    public static boolean commentType(String type) {
        if (type.equals("comment") || type.equals("userReply"))
            return true;
        return false;
    }

    // Used for field 6
    public static boolean depth(String depth) {
        try {
            int n = Integer.parseInt(depth);
            if (n == 1 || n == 2 || n == 3)
                return true;
            else
                return false;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Used for field 7
    public static boolean isBool(String str) {
        return Boolean.parseBoolean(str);
    }

    // Used for field 8
    public static boolean reply(String str) {
        if (str == null || str == "" || unsignedInteger(str) == true)
            return true;
        return false;
    }

    // Used for field 10
    public static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isNullOrEmpty(String str) {
        if (str == null || str == "")
            return true;
        return false;
    }


}
