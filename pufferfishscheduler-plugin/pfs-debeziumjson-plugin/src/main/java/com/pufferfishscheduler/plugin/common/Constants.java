package com.pufferfishscheduler.plugin.common;

/**
 * 常量类
 */
public class Constants {
    public static final String TIMESTAMP = "timestamp";
    public static final String STRING = "string";
    public static final String NUMBER = "number";
    public static final String DATE = "date";
    public static final String BIGNUMBER = "BigNumber";
    public static final String FIELD_NAME = "fieldName";
    public static final String NAME = "name";
    public static final String DESCRIBE = "describe";
    public static final String MESSAGE_STRUCTURE = "outputFieldJson";
    public static final String SOURCE_FIELD = "sourceField";
    public static final String OUTPUT_FIELD_CONFIG = "outputFieldConfig";
    public static final String FIELDS = "fields";
    public static final String FIELD = "field";
    public static final String OP = "op";
    public static final String SAMPLE_DATA = "sampleData";
    public static final String PAY_LOAD = "payload";
    public static final String SCHEMA = "schema";
    public static final String AFTER = "after";
    public static final String BEFORE = "before";
    public static final String R = "r";
    public static final String C = "c";
    public static final String U = "u";
    public static final String D = "d";
    public static final String ANALYSIS_JSON_SAVE_FAIL = "解析Debezium组件保存失败：";
    public static final String ANALYSIS_JSON_CHECK_SUCCESS = "校验成功！";
    public static final String ANALYSIS_JSON_CONFIG_ISNULL = "配置为空！";
    public static final String ANALYSIS_JSON_READ_FAIL = "解析Debezium组件读取失败！";

    // 在 Constants 类中添加以下常量
    public static final String OP_READ = "r";
    public static final String OP_CREATE = "c";
    public static final String OP_UPDATE = "u";
    public static final String OP_DELETE = "d";

    // Schema 名称常量
    public static final String TIMESTAMP_SCHEMA_NAME = "io.debezium.time.Timestamp";
    public static final String ZONED_TIMESTAMP_SCHEMA_NAME = "io.debezium.time.ZonedTimestamp";
    public static final String APACHE_TIMESTAMP_SCHEMA_NAME = "org.apache.kafka.connect.data.Timestamp";
}
