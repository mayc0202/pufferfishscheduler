package com.pufferfishscheduler.plugin.common;

/**
 * 静态常量
 */
public interface Constants {

    /**
     * 过滤类型
     */
    interface FILTER_TYPE {
        String FILTER_1 = "1"; // java
        String FILTER_0 = "0"; // 多条件
    }

    /**
     * 数据类型
     */
    interface DataType {
        String TIMESTAMP_CODE = "Timestamp";
        int TIMESTAMP_VALUE = 1;

        String STRING_CODE = "String";
        int STRING_VALUE = 2;

        String NUMBER_CODE = "Number";
        int NUMBER_VALUE = 3;

        String INTERNET_ADDRESS_CODE = "Internet Address";
        int INTERNET_ADDRESS_VALUE = 4;

        String INTEGER_CODE = "Integer";
        int INTEGER_VALUE = 5;

        String DATE_CODE = "Date";
        int DATE_VALUE = 6;

        String BOOLEAN_CODE = "Boolean";
        int BOOLEAN_VALUE = 7;

        String BINARY_CODE = "Binary";
        int BINARY_VALUE = 8;

        String BIGNUMBER_CODE = "BigNumber";
        int BIGNUMBER_VALUE = 9;

        String UNDEFINED_CODE = "undefined";
        int UNDEFINED_VALUE = 10;
    }

    /**
     * 条件运算符
     */
    interface Condition {
        String ALL = "ALL";
        String AND = " AND ";
        String OR = " OR ";
    }

    /**
     * 操作符常量接口（基础版）
     */
    interface OperatorType {
        String less = "<";
        String lessEqual = "<=";
        String great = ">";
        String greatEqual = ">=";
        String equal = "=";
        String notEqual = "!=";
        String startWith = "START_WITH";
        String endWith = "END_WITH";
        String contains = "CONTAINS";
    }
}
