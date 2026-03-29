package com.pufferfishscheduler.plugin.common.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 日期、时间、字符串 格式化转换工具
 */
public class ConvertDateToStringUtil {

    private static Map<Integer,String> dateFormtCache = new HashMap<>();

    static {
        dateFormtCache.put(0,"yyyy/MM/dd HH:mm:ss.SSS");
        dateFormtCache.put(1,"yyyy/MM/dd HH:mm:ss.SSS XXX");
        dateFormtCache.put(2,"yyyy/MM/dd HH:mm:ss");
        dateFormtCache.put(3,"yyyy/MM/dd HH:mm:ss XXX");
        dateFormtCache.put(4,"yyyyMMddHHmmss");
        dateFormtCache.put(5,"yyyy/MM/dd");
        dateFormtCache.put(6,"yyyy-MM-dd");
        dateFormtCache.put(7,"yyyy-MM-dd HH:mm:ss");
        dateFormtCache.put(8,"yyyy-MM-dd HH:mm:ss XXX");
        dateFormtCache.put(9,"yyyyMMdd");
        dateFormtCache.put(10,"MM/dd/yyyy");
        dateFormtCache.put(11,"MM/dd/yyyy HH:mm:ss");
        dateFormtCache.put(12,"MM-dd-yyyy");
        dateFormtCache.put(13,"MM-dd-yyyy HH:mm:ss");
        dateFormtCache.put(14,"MM/dd/yy");
        dateFormtCache.put(15,"MM-dd-yy");
        dateFormtCache.put(16,"dd/MM/yyyy");
        dateFormtCache.put(17,"dd-MM-yyyy");
        dateFormtCache.put(18,"yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        dateFormtCache.put(19,"yyyy-MM-dd HH:mm:ss.SSS");
    }

    /**
     * 获取所有的日期格式
     * @return
     */
    public static Map<Integer, String> getDateFormtCache() {
        return dateFormtCache;
    }

    /**
     * 获取单个日期格式
     * @param num
     * @return
     */
    public static String getFormat(Integer num) {
        return dateFormtCache.get(num);
    }

    /**
     * 字符串转日期
     * @param dateStr
     * @param convertBefore
     * @return
     */
    public static Date convertStringToDate(String dateStr,Integer convertBefore) {
        SimpleDateFormat sdfBefore = new SimpleDateFormat(dateFormtCache.get(convertBefore));
        Date date = null;
        try {
            date = sdfBefore.parse(dateStr);
        } catch (ParseException e) {
           throw new IllegalArgumentException("ParseException:" + e.getMessage());
        }
        return date;
    }

    /**
     * 日期转字符串
     * @param date
     * @param convertAfter
     * @return
     */
    public static String convertDateToString(Date date,Integer convertAfter) {
        SimpleDateFormat sdfAfter = new SimpleDateFormat(dateFormtCache.get(convertAfter));
        String format = sdfAfter.format(date);
        return format;
    }

    /**
     * 日期转时间错
     * @param date
     * @return
     */
    public static Long convertDateToTimeStamp(Date date) {
        return date.getTime();
    }
}
