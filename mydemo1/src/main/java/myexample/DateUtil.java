package myexample;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static String dateToYMDHMSStr(Date birthday) {
//        return "0";
        return sdf.format(birthday);
    }

    public static Date strToYMDHMSDate(String toString) {
        try {
            return sdf.parse(toString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Date();
    }
}
