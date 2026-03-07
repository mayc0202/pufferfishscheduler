package com.pufferfishscheduler.common.constants;

public interface Constants {

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
        // dict.json
        String DATA_SOURCE_LAYERING = "data_source_layering"; // 数据源分层
        String CONTROL_ENCODING = "control_encoding";
        String DATABASE_CATEGORY = "database_category";
        String FAILURE_POLICY = "failure_policy";
        String NOTIFY_POLICY = "notify_policy";
        String ENABLE = "enable";
        String TASK_STATUS = "task_status";

        // 业务字典
        String DB_GROUP = "db_group";
    }

    /**
     * 任务类型
     */
    interface TASK_TYPE {
        String METADATA_TASK = "metadata_task";
        String REALTIME_TASK = "realtime_task";
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
    }

    /**
     * 数据库类型
     */
    interface DbType {
        String mysql = "mysql";
        String oracle = "oracle";
        String sqlServer = "sqlserver";
        String postgresql = "postgresql";
        String dm = "dm8";
        String starRocks = "starrocks";
        String doris = "doris";
        String cache = "cache";
        String tidb = "tidb";
        String kyuubi = "kyuubi";
        String vastbaseg100 = "vastbase_g100";
        String gaussdb = "gaussdb";
        String kingbase = "kingbasees_v8";
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
     * 
     */
    interface Category {
        String R = "R";
        String N = "N";
        String B = "B";
        String E = "E";
        String O = "O";
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
     * 数据源配置信息
     */
    interface DATABASE_PROPERTIES {

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
     * 树类型
     */
    interface TREE_TYPE {
        String GROUP = "GROUP";
        String DATABASE = "DATABASE";
    }

    /**
     * Redis key
     */
    interface REDIS_KEY {
        String CATEGORY = "category";
        String DB_BASIC = "db_basic";
    }

    String CHAT_MEMORY_CONVERSATION_ID_KEY = "chat_memory_conversation_id";

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
     * 执行状态
     */
    interface EXECUTE_STATUS {
        String BREAKPOINT = "B";
        String RUNNING = "R";
        String FAILURE = "F";
        String SUCCESS = "S";
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
     * 组件类型
     */
    interface StepMetaType {
        String TABLE_INPUT = "TableInput";
        String GENERATE_TEST_DATA = "generateTestData";
        String EXCEL_INPUT = "excelInput";
        String CSV_INPUT = "csvInput";
        String JSON_INPUT = "jsonInput";
        String MONGODB_INPUT = "mongoDBInput";
        String FIELD_SELECT = "fieldSelect";
        String API_INPUT = "apiInput";
        String KAFKA_CONSUMER_INPUT = "KafkaConsumerInput";
        String RECORDS_FROM_STREAM = "RecordsFromStream";
        String REDIS_INPUT = "RedisInput";

        String TABLE_OUTPUT = "TableOutput";
        String INSERT_OR_UPDATE = "insertOrUpdate";
        String UPDATE = "update";
        String EXCEL_OUTPUT = "excelOutput";
        String JSON_OUTPUT = "jsonOutput";
        String MONGODB_OUTPUT = "mongoDBOutput";
        String API_OUTPUT = "apiOutput";
        String FILE_DOWNLOAD = "fileDownload";
        String KAFKA_PRODUCER_OUTPUT = "KafkaProducerOutput";
        String REDIS_OUTPUT = "RedisOutput";
        String STARROCKS_OUTPUT = "StarRocksOutput";
        String DORIS_OUTPUT = "DorisOutput"; // DorisOutput输出

        String JAVA_CODE = "JavaCode";
        String EXECUTE_SQL_SCRIPT = "executeSQL";

        String CONDITIONAL_JAVA = "conditionalJava";
        String PROCESS_BRANCH = "processBranch";

        String GET_VARIABLES = "geVariables";
        String SET_VARIABLES = "setVariables";

        String SYSTEM_DATE = "SystemDate";
        String FORMULA = "formula";
        String DENORMALISER = "Denormaliser"; // 行转列
        String NORMALISER = "Normaliser"; // 列转行
        String SPLITFIELDTOROWS3 = "SplitFieldToRows3"; // 字段拆分为多行
        String FIELDSPLITTER = "FieldSplitter"; // 字段拆分为多列
        String GROUPBY = "GroupBy"; // 分组
        String FILTER = "filter"; // 过滤
        String DATA_CLEAN = "dataclean"; // 清洗
        String DEBEZIUM_JSON = "DebeziumJson"; // Debezium格式JSON

        String FTP_UPLOAD = "FTPUpload"; // FTP上传
        String FTP_DOWNLOAD = "FTPDownload"; // FTP下载

        String WRITE_TO_LOG = "WriteToLog";
    }
}
