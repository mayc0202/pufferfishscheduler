package com.pufferfishscheduler.plugin.common.enums;

public enum DateFormatType {
    FORMAT0("yyyy/MM/dd HH:mm:ss.SSS"),
    FORMAT1("yyyy/MM/dd HH:mm:ss.SSS XXX"),
    FORMAT2("yyyy/MM/dd HH:mm:ss"),
    FORMAT3("yyyy/MM/dd HH:mm:ss XXX"),
    FORMAT4("yyyyMMddHHmmss"),
    FORMAT5("yyyy/MM/dd"),
    FORMAT6("yyyy-MM-dd"),
    FORMAT7("yyyy-MM-dd HH:mm:ss"),
    FORMAT8("yyyy-MM-dd HH:mm:ss XXX"),
    FORMAT9("yyyyMMdd"),
    FORMAT10("MM/dd/yyyy"),
    FORMAT11("MM/dd/yyyy HH:mm:ss"),
    FORMAT12("MM-dd-yyyy"),
    FORMAT13("MM-dd-yyyy HH:mm:ss"),
    FORMAT14("MM/dd/yy"),
    FORMAT15("MM-dd-yy"),
    FORMAT16("dd/MM/yyyy"),
    FORMAT17("dd-MM-yyyy"),
    FORMAT18("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
    FORMAT19("yyyy-MM-dd HH:mm:ss.SSS");

    private String description;

    DateFormatType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }
}
