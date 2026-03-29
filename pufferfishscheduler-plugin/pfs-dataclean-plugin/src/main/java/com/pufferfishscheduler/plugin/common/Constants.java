package com.pufferfishscheduler.plugin.common;

/**
 * 静态常量
 */
public interface Constants {

    String REQUIRES = "requires"; // 必填
    String SELECT_ARRAY = "selectArray"; // 条件集合

    String MAPPING_TYPE = "mappingType"; // 映射方式 0 手动配置 1 数据库字典表 2 自定义sql 3 标准库

    // 手工配置
    String FIELD_LIST = "fieldList"; // 参数配置集合

    String SQL_CONTEXT = "sqlContext"; // 自定义sql

    // 标准库
    String TRANSFORM_TYPE_2 = "transformType"; // 转换方式 0 代码值转为名称  1 名称转为代码值
    String CODE = "code"; // 代码
    String VALUE_NAME = "valueName"; // 值名称
    String STANDARD_NAME = "name"; // 代码名称

    Integer TRANS_VALUE_1 = 1; // 转换值方式
    Integer TRANS_VALUE_2 = 2; // 转换值方式

    String MD5 = "MD5";

    String FIELD = "field";
    String FIELDS = "fields";
    String NAME = "name";
    String DESCRIBE = "describe";
    String DATA = "data";
    String VALUE = "value";
    String FIELD_TYPE = "field_type"; // 字段类型
    String FIELD_NAME = "field_name"; // 字段名称
    String DATA_TYPE = "data_type"; // 数据类型
    String DATATYPE = "dataType"; // 数据类型
    String FIELD_DESCRIPTION = "field_description";
    String CLEAN_TYPE = "clean_type"; // 转换方式
    String PROCESSOR_CLASS = "processor_class"; // 处理器全路径
    String PARAMS = "params";
    String RENAME = "rename";
    String RENAME_TYPE = "renameType";
    String RULEID = "ruleId";
    String ADDRESS = "address";
    String APIKEY = "apiKey";
    String APIROUTE = "apiRoute";


    String START = "start"; // 开始位置
    String END = "end"; // 截至文职
    String NULL = "null"; // 空值

    String REPLACE_TYPE = "replaceType"; // 替换方式
    String REPLACE_CONTENT = "replaceContent"; // 替换的内容
    String SEARCH_CONTENT = "searchContent"; // 查找的内容

    String PADDING_POSITION = "paddingPosition"; // 填充位置 填充在左边 填充在右边
    String L = "L"; // 填充在左边
    String R = "R"; // 填充在右边
    String PADDING_CONTENT = "paddingContent"; // 填充的内容
    String PADDING_LENGTH = "paddingLength"; // 填充的长度

    String ROUND_TYPE = "roundType"; // 舍位方式 0 四舍五入 1 截取
    String INTERCEPTNUM = "interceptNum"; // 截取位数
    String DIVISOR = "divisor"; // 除数 浮点数

    String SPECIFY_VALUE = "specifyValue"; // 指定值
    String SPECIFY_CHARACTERS = "specialCharacters"; // 特殊字符

    String CONVERT_TYPE = "convertType"; // 转换方式 0 全部转大写 1 全部转小写

    String DESENSITIZE_TYPE = "desensitizeType"; // 脱敏方式
    String DESENSITIZE_SYMBOL = "*"; // 脱敏方式

    String DELETE_TYPE = "deleteType"; // 删除方式 0 删除首尾 1 删除所有
    String DELETE_CONTENT = "deleteContent"; // 删除的内容 0 空白字符串 1 空格

    String CONVERT_BEFORE = "convertBefore"; // 转换前日期格式
    String CONVERT_AFTER = "convertAfter"; // 转换后日期格式

    String DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy"; // Date US格式

    String TRANSFORM_TYPE_1 = "transformType"; // 转换方式 0 阿拉伯转中文 1 中文转阿拉伯

    String RENAME_List = "renameList";
    String JAVA_CODE = "javaCode"; // java代码

    // 提示消息
    String CHECK_DATA_CLEAN_SUCCESS = "校验成功！";
    String DATA_CLEAN_CONFIG_ISNULL = "清洗配置为空！";
    String DATA_CLEAN_READ_FAIL = "清洗转换组件读取失败！";
    String DATA_CLEAN_SAVE_FAIL = "清洗转换组件保存失败：";
    String API_USED_FAIL = "接口调用失败！";
    String JDBC_NOT_FIND = "JDBC 驱动未找到！";
    String DATABASE_OPERATE_EXCEPTIPN = "数据库操作异常：";

    String Index_OUT_OF_BOUNDS = "Index out Of bounds exception：";
    String META_IS_NOT_CONTAIN_KEY = "Metadata doesn't contain：";
    String ANALYSIS_EXCEPTION = " Parsing exceptions：";
    String INVALID_VALUE = "请校验参数列表的的值：";
    String REQUIRES_VALUE_IS_NULL = " value is required,but value is null!";
    String AND = " and ";
    String OR = " or ";
    String TRUNCATION_START = "字符串截取开始位置";
    String TRUNCATION_END = "字符串截取截止位置";


    // 数据类型
    String TIMESTAMP = "Timestamp";
    String STRING = "String";
    String NUMBER = "Number";
    String INTERNET_ADDRESS = "Internet Address";
    String INTEGER = "Integer";
    String DATE = "Date";
    String BOOLEAN = "Boolean";
    String BINARY = "Binary";
    String BIGNUMBER = "BigNumber";
    String UNDEFINED = "undefined";

    /**
     * 规则处理器ID
     */
    interface PROCESSOR_ID {
        /**
         * Java自定义规则处理器ID
         */
        Integer JAVA_CUSTOM = 18;

        /**
         * 值映射规则处理器ID
         */
        Integer VALUE_MAPPING = 19;
    }

    /**
     * 数据库类型
     */
    interface DATABASE_TYPE {
        String MYSQL = "MySQL";
        String ORACLE = "Oracle";
        String SQL_SERVER = "SQLServer";
        String POSTGRESQL = "PostgresSQL";
        String DM8 = "DM8";
        String STAR_ROCKS = "StarRocks";
        String DORIS = "Doris";
    }


    interface DATABASE_EXT_CONFIG{
        String BE_ADDRESS = "beAddress";
        String FE_ADDRESS = "feAddress";
        String ORACLE_CONNECT_TYPE = "connectType";

    }

    interface ORACLE_CONNECT_TYPE{
        String SID= "sid";
        String SERVICE = "service";
    }

    /**
     * 条件运算符
     */
    interface CONDITION {
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
        String like = "CONTAINS";
    }
}
