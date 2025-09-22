package com.pufferfishscheduler.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Mayc
 * @since 2025-08-15  00:03
 */
public class ConnectorUtil {
    private static Pattern dataTypePattern;

    /**
     * 通过正则匹配括号内的字符串 限制单个括号
     * @param msg
     * @return
     */
    public static String getMsg(String msg) {

        Pattern p = Pattern.compile("(?<=\\()(\\d+\\,?\\d*)(?=\\))");
        Matcher m = p.matcher(msg);
        String result=null;
        if(m.find()) {
            String match = m.group(0);
            String[] group = match.split(",");
            result = group[0];
        }
        return result;
    }

    /**
     * 获取精度
     * @param msg
     * @return
     */
    public static String getPrecision(String msg) {

        Pattern p = Pattern.compile("(?<=\\()(\\d+\\,?\\d*)(?=\\))");
        Matcher m = p.matcher(msg);
        String result=null;
        if(m.find()) {
            String match = m.group(0);
            String[] group = match.split(",");
            if(group.length>1) {
                result = group[1];
            }
        }
        return result;
    }

    /**
     * 元数据字段COLUMN_TYPE,DATA_TYPE的区别
     * DATA_TYPE 是一个标准的 SQL 字段，它表示列的数据类型。它是一个简单的字符串，描述了列的基本数据类型，通常是数据库系统中预定义的数据类型名称。
     * COLUMN_TYPE 是一个扩展字段，它提供了更详细的列类型信息。它不仅包括数据类型，还可能包括长度、精度。
     * 方法表示从COLUMN_TYPE中提取数据类型
     * 例如：varchar(100) 返回varchar
     * @param columnType
     * @return
     */
    public static String getDataTypeFromColumnType(String columnType) {
        if(dataTypePattern == null){
			/*
			 * ([a-zA-Z0-9]+)：匹配一个或多个字母或数字，并将其捕获为第一个捕获组。这用于匹配数据类型如 varchar、char
			 * (?:\([^)]*\))?:
			 * 	\( ... \)：匹配括号内的任意内容（包括空格和逗号）。
			 	[^)]*：匹配括号内的任意字符，但不包括右括号 )。
			 	?:：表示这是一个非捕获组（即不保存匹配的内容）。
			 	?：表示这个组是可选的，即括号部分可以存在也可以不存在。
			 */
            dataTypePattern = Pattern.compile("([a-zA-Z0-9]+)(?:\\([^)]*\\))?");
        }
        Matcher m = dataTypePattern.matcher(columnType);
        String result=null;
        if(m.find()) {
            result = m.group(1);
        }
        return result;
    }
}
