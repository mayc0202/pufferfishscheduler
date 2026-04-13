package com.pufferfishscheduler.common.constants;

public interface Constants {

    /**
     * 系统操作信息
     */
    interface SYS_OP_INFO {
        /**
         * 系统调度器账号
         */
        String SYSTEM_ACCOUNT = "SYSTEM";
        /**
         * 系统调度器名称
         */
        String SYSTEM_NAME = "系统";
    }

    // 默认日志格式
    String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    String DEFAULT_DATE_FORMAT_2 = "yyyyMMdd";
    String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    // 默认编码UTF-8
    String CONTROL_ENCODING = "UTF-8";
    String TIME_ZONE = "Etc/GMT-8";

    String DICT_FILE = "dict.json";

    /**
     * Header 配置
     */
    interface HEADER_CONFIG {
        String CONTENT_LENGTH = "Content-Length";
        String CONTENT_DISPOSITION = "Content-Disposition";
        String CACHE_CONTROL = "Cache-Control";
        String PRAGMA = "Pragma";
        String EXPIRES = "Expires";
    }

    /**
     * Content 类型
     */
    interface CONTENT_TYPE {
        String TXT = "text/plain;charset=UTF-8";
        String PDF = "application/pdf";
        String ZIP = "application/zip";
        String XLS = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        String DOC = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        String PPT = "application/vnd.openxmlformats-officedocument.presentationml.presentation";
        String JPG = "image/jpeg";
        String PNG = "image/png";
        String GIF = "image/gif";
        String XML = "application/xml";
        String JSON = "application/json";
        String OCTET_STREAM = "application/octet-stream";
    }

    /**
     * 文件类型
     */
    interface FILE_TYPE {
        String MD = "md";
        String TXT = "txt";
        String PDF = "pdf";
        String ZIP = "zip";
        String CSV = "csv";
        String XLSX = "xlsx";
        String XLS = "xls";
        String DOCX = "docx";
        String DOC = "doc";
        String PPTX = "pptx";
        String PPT = "ppt";
        String JPG = "jpg";
        String JPEG = "jpeg";
        String PNG = "png";
        String GIF = "gif";
        String XML = "xml";
        String JSON = "json";
    }

    /**
     * 字典标识
     */
    interface DICT {
        // 数据类型
        String DATA_TYPE = "data_type";
        // 数据源分层
        String DATA_SOURCE_LAYERING = "data_source_layering";
        // 控制编码
        String CONTROL_ENCODING = "control_encoding";
        // 数据库分类
        String DATABASE_CATEGORY = "database_category";
        // 失败策略
        String FAILURE_POLICY = "failure_policy";
        // 通知策略
        String NOTIFY_POLICY = "notify_policy";
        // 启用状态
        String ENABLE = "enable";
        // 任务状态s
        String TASK_STATUS = "task_status";
        // 业务字典
        String DB_GROUP = "db_group";
        // CDC引擎类型
        String CDC_ENGINE_TYPE = "cdc_engine_type";
        // 作业管理状态
        String JOB_MANAGE_STATUS = "job_manage_status";
        // 调度类型
        String SCHEDULE_TYPE = "schedule_type";
        // 告警类型
        String ALERT_METHOD = "alert_method";
        // 执行状态
        String EXECUTE_STATUS = "execute_status";
        // 映射类型
        String MAPPING_TYPE = "mapping_type";
        // 知识类型
        String KNOWLEDGE_TYPE = "knowledge_type";
        // 电子表格类型
        String SPREAD_SHEET_TYPE = "spread_sheet_type";
        // 操作类型
        String OPERATOR_TYPE = "operator_type";
    }

    /**
     * 任务类型
     */
    interface TASK_TYPE {
        String METADATA_TASK = "metadata_task";
        String TRANS_TASK = "trans_task";
        /**
         * Worker 侧停止正在执行的转换（与 TRANS_TASK 共用 trans_task.id）
         */
        String TRANS_TASK_STOP = "trans_task_stop";
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

        String REDIS = "Redis";
        String MONGODB = "MongoDB";

        String KAFKA = "Kafka";
        String RABBITMQ = "RabbitMQ";
    }

    /**
     * db_tale_properties 属性类型
     * 1-物理主键字段；
     * 2-逻辑主键字段；
     * 3-业务标识字段；
     * 4-数据同步时间字段；
     * 5-创建时间字段；
     * 6-更新时间字段；
     * 7-数据同步方式;
     * 8-增量标识字段;
     * 9-数据同步周期;
     */
    interface TABLE_PROPERTIES {
        String PRIMARY_KEY = "1";
        String LOGIC_KEY = "2";
        String BUSINESS_FLAG = "3";
        String DATA_SYNC_TIME = "4";
        String CREATE_TIME = "5";
        String UPDATE_TIME = "6";
        String SYNC_TYPE = "7";
        String SYNC_INR_COLUMN = "8";
        String DATA_SYNC_CYCLE = "9";
        String RELATE_BIZ_FLAG = "10";

    }

    /**
     * 任务配置类型
     */
    interface JOB_CONFIG_TYPE {
        String SQL = "1";
        String VISUAL = "2";
    }

    /**
     * 增量类型
     */
    interface INCREMENT_TYPE {
        String NUMBER_TYPE = "1";
        String DATE_TYPE = "2";
    }

    /*
     * 表输出分区类型
     */
    interface PARTITIONING_TYPE {
        String TIME = "1"; // 按时间
        String CUSTOMIZE = "2"; // 自定义
    }

    /**
     * 表输出分区时间类型
     */
    interface PARTITIONING_TIME {
        String MONTH = "m"; // 按月
        String DAY = "d"; // 按天
    }

    /**
     * 数据库大类
     */
    interface Category {
        String R = "1"; // 关系型数据库
        String N = "2"; // 非关系型数据库
        String M = "3"; // 消息型数据库
        String F = "4"; // FTP类型
        String O = "5"; // OSS
    }

    /**
     * kettle 数据库类型
     */
    interface Kettle_DataBaseType {
        String mysql = "MYSQL";
        String oracle = "ORACLE";
        String sqlServer = "MSSQL";
        String postgresql = "POSTGRESQL";
        String generic = "GENERIC";
    }

    /**
     * 缓存配置
     */
    interface CACHE_CONFIG {
        String ATRRIBUTE_CUSTOM_URL = "CUSTOM_URL";
        String ATRRIBUTE_CUSTOM_DRIVER_CLASS = "CUSTOM_DRIVER_CLASS";
    }

    /**
     * Debezium JSON 字段
     */
    interface DEBEZIUM_JSON {
        String FIELDS = "fields";
        String FIELD = "field";
        String OP = "op";
        String SAMPLE_DATA = "sampleData";
        String PAY_LOAD = "payload";
        String SCHEMA = "schema";
        String AFTER = "after";
        String BEFORE = "before";
        String R = "r";
        String C = "c";
        String U = "u";
        String D = "d";
    }

    /**
     * FTP/FTPS类型
     */
    interface FTP_TYPE {
        String FTP = "FTP";
        String FTPS = "FTPS";
    }

    /**
     * FTP/FTPS被动模型
     */
    interface MODE_TYPE {
        String ACTIVE = "1";
        String PASSIVE = "2";
    }

    String FILE_SEPARATOR = "/";
    String DIRECTORY_SIZE = "-";

    /**
     * FTP文件类型
     */
    interface FTP_FILE_TYPE {
        String DIRECTORY = "DIRECTORY";
        String FILE = "FILE";
    }

    /**
     * FTP配置
     */
    interface FTP_PROPERTIES {
        String MODE = "mode";
        String CONTROL_ENCODING = "controlEncoding";
    }

    /**
     * 数据类型
     */
    interface DATA_TYPE {
        String BLOB = "BLOB";
        String CLOB = "CLOB";
        String DATE = "DATE";
        String TIMESTAMP = "TIMESTAMP";
        String VARCHAR = "VARCHAR";
        String INT = "INT";
        String BIT = "BIT";
        String CHAR = "CHAR";
        String TEXT = "TEXT";
        String REAL = "REAL";
        String BINARY = "BINARY";
        String VARBINARY = "VARBINARY";
        String DOUBLE = "DOUBLE";
    }

    /**
     * 数据源扩展配置
     */
    interface DATABASE_EXT_CONFIG {
        String BE_ADDRESS = "beAddress";
        String FE_ADDRESS = "feAddress";
        String ORACLE_CONNECT_TYPE = "connectType";

    }

    interface ORACLE_CONNECT_TYPE {
        String SID = "sid";
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

    /**
     * 系统内置角色名（与 role.name 一致）
     */
    interface ROLE_NAME {
        String ADMIN = "admin";
        String EDITOR = "editor";
    }

    /**
     * token
     */
    interface TOKEN_CONFIG {
        // 定义标准的claim名称常量
        String CLAIM_USER_ID = "userId";
        String CLAIM_ACCOUNT = "account";

        String TOKEN = "Pufferfish-Token";
        String BEARER_PREFIX = "Pufferfish-Bearer:";
        String SSO_KEY_PREFIX = "sso:user:";
        String USER_STATUS_PREFIX = "user:status:";
        String BLACKLIST_KEY_PREFIX = "jwt:blacklist:";
        String RATE_LIMIT_PREFIX = "rate_limit:";
        String AUTHORIZATION = "Authorization";
    }

    /**
     * 删除标记
     */
    interface DELETE_FLAG {
        Boolean TRUE = true;
        Boolean FALSE = false;
    }

    /**
     * 启用状态
     */
    interface ENABLE_FLAG {
        String TRUE = "1";
        String FALSE = "0";
    }

    /**
     * 树类型
     */
    interface TREE_TYPE {
        String GROUP = "GROUP";
        String RULE = "RULE";
        String DATABASE = "DATABASE";
        String TRANS_FLOW = "TRANS_FLOW";
        String MENU = "menu";
        String PLUGIN = "plugin";
    }

    /**
     * Redis key
     */
    interface REDIS_KEY {
        String CATEGORY = "category";
        String DB_BASIC = "db_basic";

        /**
         * 实时统计：累计量 key 前缀，完整 key: rt:stats:task:{taskId}:table:{tableMapperId}
         */
        String RT_STATS_PREFIX = "rt:stats:task:";
        /**
         * 实时统计：按小时日志 key 前缀，完整 key: rt:log:task:{taskId}:table:{tableMapperId}:{yyyyMMdd}:{HH}
         */
        String RT_LOG_PREFIX = "rt:log:task:";
    }

    /**
     * 消息类型
     */
    interface MESSAGE_TYPE {
        String USER = "user";
        String ASSISTANT = "assistant";
        String SYSTEM = "system";
        String TOOL = "tool";
        String UNKNOWN = "unknown";
    }

    interface CONVERSATION_TYPE {
        String CHAT = "chat";
    }

    /**
     * kafka状态
     */
    interface KAFKA_STATUS {
        String RUNNING = "RUNNING";
        String STOP = "STOP";
    }

    /**
     * 数据处理阶段
     */
    interface STAGE {
        String CONVERGE = "1"; //归集
        String INTEGRATION = "2"; //清洗
        String MT_TASK = "8"; //多表同步任务
    }

    /**
     * 执行状态
     */
    interface EXECUTE_STATUS {
        String RUNNING = "R";
        String FAILURE = "F";
        String SUCCESS = "S";
        String TERMINATE = "T";//终止
    }

    /**
     * 实时任务状态
     */
    interface RT_TASK_STATUS {
        String RUNNING = "R";
        String FAILURE = "F";
        String STOP = "T";
        String START = "S";
    }

    /**
     * 任务管理任务的状态
     */
    interface JOB_MANAGE_STATUS {
        /**
         * 未启动（首次添加进来时此状态）
         */
        String INIT = "INIT";
        String INIT_TXT = "未启动";

        /**
         * 运行中
         */
        String RUNNING = "RUNNING";
        String RUNNING_TXT = "运行中";

        /**
         * 已停止
         */
        String STOP = "STOP";
        String STOP_TXT = "已停止";

        /**
         * 异常
         */
        String FAILURE = "FAILURE";
        String FAILURE_TXT = "失败";

        /**
         * 启动中
         */
        String STARTING = "STARTING";
        String STARTING_TXT = "启动中";

        /**
         * 停止中
         */
        String STOPPING = "STOPPING";
        String STOPPING_TXT = "停止中";
    }


    String TRANS = "TRANS";

    String START = "start";

    String END = "end";

    String DEFAULT = "default";

    /**
     * 执行类型
     */
    interface EXECUTE_TYPE {
        String TIMING = "1";
        String FLOW = "2";
        String STREAM = "3";
    }

    /**
     * 模板标识
     */
    interface TEMPLATE {
        String VALUE_MAPPING = "value-mapping-template.xlsx";
    }

    /**
     * 规则管理
     */
    interface RULE_MANAGER {
        Integer GENERIC_TYPE = 1; // 通用
        String GENERIC_TXT = "通用"; // 通用
        Integer PUBLIC_TYPE = 2; // 公共的
        String PUBLIC_TXT = "公共"; // 公共的
        Integer CUSTOM_TYPE = 3; // 自定义的
        String CUSTOM_TXT = "自定义"; // 自定义的
    }

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
     * 值映射类型
     */
    interface MAPPING_TYPE {
        Integer MANUAL = 0; // 手工
        Integer DATABASE_DICT_TABLE = 1; // 字典表
        Integer CUSTOM_SQL = 2; // 自定义sql
    }

    /**
     * 从数据源获取数据的类型
     */
    interface FROM_SOURCE_TYPE {
        Integer FROM_FIELD = 1; // 从字段获取
        Integer FROM_FILE = 2; // 从文件源获取
    }

    /**
     * 文件源类型
     * 1：FTP文件
     * 2：本地文件
     */
    interface FILE_SOURCE_TYPE {
        String FTP_FILE = "FTP_FILE";
        String LOCAL_FILE = "LOCAL_FILE";
    }

    /**
     * 记录生成器类型
     */
    interface RECORD_GENERATOR_TYPE {
        Integer BATCH = 1; // 批量生成记录
        Integer CONTINUOUS = 2; // 持续生成记录
    }

    /**
     * 本地文件类型
     */
    interface LOCAL_FILE_TYPE {
        String ALL = "1";
        String DIR = "2";
        String ZIP = "3";
    }

    /**
     * 组件类型
     */
    interface StepMetaType {
        String TABLE_INPUT = "TableInput";
        String ROW_GENERATOR = "RowGenerator";
        String EXCEL_INPUT = "ExcelInput";
        String CSV_INPUT = "CsvInput";
        String JSON_INPUT = "JsonInput";
        String MONGODB_INPUT = "MongoDBInput";
        String API_INPUT = "ApiInput";
        String KAFKA_CONSUMER_INPUT = "KafkaConsumerInput";
        String RABBITMQ_CONSUMER_INPUT = "RabbitMqConsumerInput";
        String RECORDS_FROM_STREAM = "RecordsFromStream";
        String REDIS_INPUT = "RedisInput";

        String TABLE_OUTPUT = "TableOutput";
        String INSERT_OR_UPDATE = "InsertOrUpdate";
        String EXCEL_OUTPUT = "ExcelOutput";
//        String JSON_OUTPUT = "JsonOutput";
//        String MONGODB_OUTPUT = "MongoDBOutput";
        String API_OUTPUT = "ApiOutput";
        String FILE_DOWNLOAD = "FileDownload";
        String KAFKA_PRODUCER_OUTPUT = "KafkaProducerOutput";
        String RABBITMQ_PRODUCER_OUTPUT = "RabbitMqProducerOutput";
        String REDIS_OUTPUT = "RedisOutput";
        String DORIS_OUTPUT = "DorisOutput"; // DorisOutput输出

        String JAVA_CODE = "JavaCode"; // Java脚本
        String SQL_SCRIPT = "SQLScript"; // 执行SQL脚本

        String JAVA_CONDITIONAL = "JavaCondition"; // 条件Java
        String PROCESS_BRANCH = "ProcessBranch"; // 多分支

        String SYSTEM_DATE = "SystemDate";
        String DENORMALIZED = "Denormalized"; // 行转列
        String NORMALISER = "Normaliser"; // 列转行
        String SORT_ROWS = "SortRows"; // 排序
        String FIELD_SELECT = "FieldSelect";
        String SPLIT_FIELD_TO_ROWS = "SplitFieldToRows"; // 字段拆分为多行
        String FIELD_SPLIT_TO_COLUMNS = "FieldSplitter"; // 字段拆分为多列
        String DATA_FILTER = "DataFilter"; // 数据过滤
        String DATA_CLEAN = "DataClean"; // 清洗
        String DEBEZIUM_JSON = "DebeziumJson"; // Debezium格式JSON

        String FTP_UPLOAD = "FTPUpload"; // FTP上传
        String FTP_DOWNLOAD = "FTPDownload"; // FTP下载

        String WRITE_TO_LOG = "WriteToLog"; // 写入日志
        String GET_VARIABLES = "GetVariables"; // 获取变量
        String SET_VARIABLES = "SetVariables"; // 设置变量
    }

    /**
     * 提交类型
     */
    interface COMMIT_TYPE {
        String MANUAL_COMMIT = "1";
        String AUTO_COMMIT = "2";
    }

    /**
     * kafka字段
     */
    interface KAFKA_FIELD {
        String KEY = "key";
        String MESSAGE = "message";
        String TOPIC = "topic";
        String PARTITION = "partition";
        String OFFSET = "offset";
        String TIMESTAMP = "timestamp";
    }

    /**
     * RabbitMQ 消费输出列（与前端 fieldList.rabbitName 对齐）
     */
    interface RABBITMQ_FIELD {
        String MESSAGE = "message";
        String ROUTING_KEY = "routingKey";
        String MESSAGE_ID = "messageId";
        String DELIVERY_TAG = "deliveryTag";
        String EXCHANGE = "exchange";
        String TIMESTAMP = "timestamp";
    }

    /**
     * RabbitMQ 公共配置
     */
    interface RABBITMQ_COMMON_CONFIG {
        String DEFAULT_PORT = "5672";
        String DEFAULT_USERNAME = "guest";
        String DEFAULT_PASSWORD = "guest";
    }
}
