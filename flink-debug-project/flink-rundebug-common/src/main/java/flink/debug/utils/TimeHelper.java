package flink.debug.utils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @projectName: JavaApiStudy
 * @className: DateTimeUtils
 * @description: com.bigdata.java.timedate.DateTimeUtils
 * @author: jiaqing.he
 * @date: 2023/3/22 11:20
 * @version: 1.0
 */
public class TimeHelper implements Serializable {

    private static final String COMM_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String COMM_HOUR_DETAIL_FORMAT = "HH:mm:ss.SSS";
    public static String getCommFormat() {
        return COMM_FORMAT;
    }

    private static SimpleDateFormat commSimpleFormatter;
    public static SimpleDateFormat getCommSimpleFormatter() {
        if (null == commSimpleFormatter) {
            synchronized (TimeHelper.class) {
                if (null == commSimpleFormatter) {
                    commSimpleFormatter =new SimpleDateFormat(COMM_FORMAT);
                }
            }
        }
        return commSimpleFormatter;
    }

    private static DateTimeFormatter commDateTimeFormatter;
    public static DateTimeFormatter getCommDateTimeFormatter() {
        if (null == commDateTimeFormatter) {
            synchronized (TimeHelper.class) {
                if (null == commDateTimeFormatter) {
                    commDateTimeFormatter = DateTimeFormatter.ofPattern(COMM_FORMAT);
                }
            }
        }
        return commDateTimeFormatter;
    }

    private static SimpleDateFormat commSimpleHourFormatter;
    public static SimpleDateFormat getCommSimpleHourFormatter() {
        if (null == commSimpleHourFormatter) {
            synchronized (TimeHelper.class) {
                if (null == commSimpleHourFormatter) {
                    commSimpleHourFormatter =new SimpleDateFormat(COMM_HOUR_DETAIL_FORMAT);
                }
            }
        }
        return commSimpleHourFormatter;
    }


    public static class Format {

        public static String formatAsCommStr(long timeMillis) {
            return getCommSimpleFormatter().format(new Date(timeMillis));
        }

        public static String formatAsHHmmssSSS(long timeMillis) {
            return getCommSimpleHourFormatter().format(new Date(timeMillis));
        }



        public static String formatAsTargetStr(Long timestamp, String format){
            return new SimpleDateFormat(format).format(new Date(timestamp));
        }

    }


    public static class Parser {


    }


}
