package com.pufferfishscheduler.common.utils;

import com.pufferfishscheduler.common.exception.BusinessException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;


/**
 * 日期时间工具类
 * 提供日期格式转换、计算、解析等功能
 * 兼容传统Date和Java 8+新时间API
 *
 * @author Mayc
 * @since 2025-09-21  01:30
 */
public class DateUtil {

    // 常用日期格式模式
    private static final String[] PARSE_PATTERNS = {
            "yyyy-MM-dd", "yyyy年MM月dd日", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm", "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss",
            "yyyy/MM/dd HH:mm", "yyyyMMdd"
    };

    // 标准日期格式
    public static final String STANDARD_DATE_FORMAT = "yyyy-MM-dd";
    public static final String STANDARD_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String COMPACT_DATE_FORMAT = "yyyyMMdd";
    public static final String CRON_EXPRESSION_FORMAT = "ss mm HH dd MM ? yyyy";

    // Java 8+ 日期格式化器
    public static final DateTimeFormatter STANDARD_DATE_FORMATTER = DateTimeFormatter.ofPattern(STANDARD_DATE_FORMAT);
    public static final DateTimeFormatter STANDARD_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(STANDARD_DATETIME_FORMAT);
    public static final DateTimeFormatter COMPACT_DATE_FORMATTER = DateTimeFormatter.ofPattern(COMPACT_DATE_FORMAT);

    private DateUtil() {
        throw new UnsupportedOperationException("工具类不允许实例化");
    }

    /**
     * 字符串转换为日期对象
     *
     * @param dateStr 日期字符串
     * @return 日期对象
     * @throws BusinessException 日期格式不合法时抛出
     */
    public static Date parseStringToDate(String dateStr) {
        if (StringUtils.isBlank(dateStr)) {
            throw new BusinessException("日期字符串不能为空");
        }

        try {
            return DateUtils.parseDate(dateStr, STANDARD_DATE_FORMAT);
        } catch (ParseException e) {
            throw new BusinessException(String.format("日期格式不合法，期望格式: %s", STANDARD_DATE_FORMAT));
        }
    }

    /**
     * 获取指定日期的下一天
     *
     * @param dateStr 日期字符串
     * @return 下一天的日期
     */
    public static Date getNextDay(String dateStr) {
        Date date = parseStringToDate(dateStr);
        return getNextDay(date);
    }

    /**
     * 获取指定日期的下一天
     *
     * @param date 日期对象
     * @return 下一天的日期
     */
    public static Date getNextDay(Date date) {
        Objects.requireNonNull(date, "日期对象不能为空");
        return DateUtils.addDays(date, 1);
    }

    /**
     * 获取指定日期的前一天
     *
     * @param date 日期对象
     * @return 前一天的日期
     */
    public static Date getPreviousDay(Date date) {
        Objects.requireNonNull(date, "日期对象不能为空");
        return DateUtils.addDays(date, -1);
    }

    /**
     * 日期对象转换为字符串
     *
     * @param date 日期对象
     * @return 格式化后的字符串
     */
    public static String formatDateToString(Date date) {
        if (date == null) {
            return null;
        }
        return DateFormatUtils.format(date, STANDARD_DATE_FORMAT);
    }

    /**
     * 获取日历的年份
     *
     * @param calendar 日历对象
     * @return 年份字符串
     */
    public static String getYear(Calendar calendar) {
        Objects.requireNonNull(calendar, "日历对象不能为空");
        return String.valueOf(calendar.get(Calendar.YEAR));
    }

    /**
     * 获取日历的月份
     *
     * @param calendar 日历对象
     * @return 月份字符串（1-12）
     */
    public static String getMonth(Calendar calendar) {
        Objects.requireNonNull(calendar, "日历对象不能为空");
        // Calendar月份从0开始，需要+1
        int month = calendar.get(Calendar.MONTH) + 1;
        return String.valueOf(month);
    }

    /**
     * 获取日历的日期
     *
     * @param calendar 日历对象
     * @return 日期字符串
     */
    public static String getDay(Calendar calendar) {
        Objects.requireNonNull(calendar, "日历对象不能为空");
        return String.valueOf(calendar.get(Calendar.DATE));
    }

    /**
     * 按指定模式格式化日期
     *
     * @param date    日期对象
     * @param pattern 格式模式
     * @return 格式化后的字符串
     */
    public static String formatDate(Date date, String pattern) {
        if (date == null || StringUtils.isBlank(pattern)) {
            return null;
        }
        return DateFormatUtils.format(date, pattern);
    }

    /**
     * 将日期转换为Cron表达式
     *
     * @param date 日期对象
     * @return Cron表达式
     */
    public static String convertToCronExpression(Date date) {
        Objects.requireNonNull(date, "日期对象不能为空");
        return formatDate(date, CRON_EXPRESSION_FORMAT);
    }

    /**
     * 解析日期时间字符串
     *
     * @param dateTimeStr 日期时间字符串
     * @return 日期对象
     */
    public static Date parseDateTime(String dateTimeStr) {
        if (StringUtils.isBlank(dateTimeStr)) {
            return null;
        }

        try {
            return DateUtils.parseDate(dateTimeStr, STANDARD_DATETIME_FORMAT);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * Cron表达式转换为日期
     *
     * @param cronExpression Cron表达式
     * @return 日期对象
     */
    public static Date parseCronExpression(String cronExpression) {
        if (StringUtils.isBlank(cronExpression)) {
            return null;
        }

        try {
            return DateUtils.parseDate(cronExpression, CRON_EXPRESSION_FORMAT);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 日期时间对象转换为字符串
     *
     * @param date 日期对象
     * @return 格式化后的字符串
     */
    public static String formatDateTime(Date date) {
        return formatDate(date, STANDARD_DATETIME_FORMAT);
    }

    /**
     * 获取当前日期的字符串表示（yyyyMMdd）
     *
     * @return 当前日期字符串
     */
    public static String getCurrentDateString() {
        return formatDate(new Date(), COMPACT_DATE_FORMAT);
    }

    /**
     * 获取指定日期加减天数后的日期
     *
     * @param date 基准日期
     * @param days 加减天数，正数为加，负数为减
     * @return 计算后的日期
     */
    public static Date addDays(Date date, int days) {
        Objects.requireNonNull(date, "日期对象不能为空");
        return DateUtils.addDays(date, days);
    }

    /**
     * 时间戳字符串转换为日期字符串
     *
     * @param timestampStr 时间戳字符串
     * @return 格式化后的日期字符串
     */
    public static String convertTimestampToDate(String timestampStr) {
        if (StringUtils.isBlank(timestampStr)) {
            return "";
        }

        try {
            long timestamp = Long.parseLong(timestampStr.replace(",", "").trim());
            // 处理毫秒和秒级时间戳
            if (String.valueOf(timestamp).length() == 10) {
                timestamp *= 1000;
            }
            Date date = new Date(timestamp);
            return formatDateToString(date);
        } catch (NumberFormatException e) {
            return "";
        }
    }

    /**
     * 日期转换为查询用的字符串格式
     *
     * @param date 日期对象
     * @return 格式化后的字符串
     */
    public static String formatDateForQuery(Date date) {
        return formatDateToString(date);
    }

    /**
     * 日期转换为整数格式（yyyyMMdd）
     *
     * @param date 日期对象
     * @return 整数表示的日期
     */
    public static int convertDateToInt(Date date) {
        if (date == null) {
            throw new BusinessException("日期对象不能为空");
        }

        String dateStr = formatDate(date, COMPACT_DATE_FORMAT);
        try {
            return Integer.parseInt(dateStr);
        } catch (NumberFormatException e) {
            throw new BusinessException(String.format("日期[%s]转换为整数失败:%s", dateStr, e.getMessage()));
        }
    }

    /**
     * 按指定模式解析日期字符串
     *
     * @param dateStr 日期字符串
     * @param pattern 格式模式
     * @return 日期对象
     */
    public static Date parseDate(String dateStr, String pattern) {
        if (StringUtils.isBlank(dateStr) || StringUtils.isBlank(pattern)) {
            return null;
        }

        try {
            return DateUtils.parseDate(dateStr, pattern);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 计算两个日期之间的天数
     *
     * @param startDateStr 开始日期字符串
     * @param endDateStr   结束日期字符串
     * @return 相隔天数，结束日期 >= 开始日期时返回正数
     */
    public static long calculateDaysBetween(String startDateStr, String endDateStr) {
        Date startDate = parseDate(startDateStr, STANDARD_DATE_FORMAT);
        Date endDate = parseDate(endDateStr, STANDARD_DATE_FORMAT);

        if (startDate == null || endDate == null) {
            throw new BusinessException("日期格式不合法，期望格式: " + STANDARD_DATE_FORMAT);
        }

        long timeDifference = endDate.getTime() - startDate.getTime();
        return timeDifference / (1000 * 60 * 60 * 24) + 1;
    }

    /**
     * 智能解析日期字符串
     *
     * @param dateStr 日期字符串
     * @return 日期对象
     */
    public static Date smartParseDate(String dateStr) {
        if (StringUtils.isBlank(dateStr)) {
            return null;
        }

        try {
            return DateUtils.parseDate(dateStr.trim(), PARSE_PATTERNS);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 获取当前时间
     *
     * @return 当前时间
     */
    public static Date getCurrentTime() {
        return new Date();
    }

    /**
     * 判断是否为同一天
     *
     * @param date1 日期1
     * @param date2 日期2
     * @return 是否为同一天
     */
    public static boolean isSameDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            return false;
        }
        return DateUtils.isSameDay(date1, date2);
    }

    // ======================== Java 8+ 时间API相关方法 ========================

    /**
     * LocalDate转换为Date
     *
     * @param localDate LocalDate对象
     * @return Date对象
     */
    public static Date convertToDate(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    /**
     * LocalDateTime转换为Date
     *
     * @param localDateTime LocalDateTime对象
     * @return Date对象
     */
    public static Date convertToDate(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Date转换为LocalDate
     *
     * @param date Date对象
     * @return LocalDate对象
     */
    public static LocalDate convertToLocalDate(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    /**
     * Date转换为LocalDateTime
     *
     * @param date Date对象
     * @return LocalDateTime对象
     */
    public static LocalDateTime convertToLocalDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * 格式化LocalDate
     *
     * @param localDate LocalDate对象
     * @return 格式化字符串
     */
    public static String format(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return localDate.format(STANDARD_DATE_FORMATTER);
    }

    /**
     * 格式化LocalDateTime
     *
     * @param localDateTime LocalDateTime对象
     * @return 格式化字符串
     */
    public static String format(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return localDateTime.format(STANDARD_DATETIME_FORMATTER);
    }

    /**
     * 解析字符串为LocalDate
     *
     * @param dateStr 日期字符串
     * @return LocalDate对象
     */
    public static LocalDate parseLocalDate(String dateStr) {
        if (StringUtils.isBlank(dateStr)) {
            return null;
        }
        try {
            return LocalDate.parse(dateStr, STANDARD_DATE_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 解析字符串为LocalDateTime
     *
     * @param dateTimeStr 日期时间字符串
     * @return LocalDateTime对象
     */
    public static LocalDateTime parseLocalDateTime(String dateTimeStr) {
        if (StringUtils.isBlank(dateTimeStr)) {
            return null;
        }
        try {
            return LocalDateTime.parse(dateTimeStr, STANDARD_DATETIME_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 计算两个日期之间的天数差
     *
     * @param start 开始日期
     * @param end   结束日期
     * @return 天数差
     */
    public static long daysBetween(LocalDate start, LocalDate end) {
        if (start == null || end == null) {
            return 0;
        }
        return ChronoUnit.DAYS.between(start, end);
    }

    /**
     * 获取当前LocalDate
     *
     * @return 当前日期
     */
    public static LocalDate getCurrentLocalDate() {
        return LocalDate.now();
    }

    /**
     * 获取当前LocalDateTime
     *
     * @return 当前日期时间
     */
    public static LocalDateTime getCurrentLocalDateTime() {
        return LocalDateTime.now();
    }
}
