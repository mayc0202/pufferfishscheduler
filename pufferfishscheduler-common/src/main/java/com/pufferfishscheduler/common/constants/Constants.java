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
        String TXT = "txt";
        String PDF = "pdf";
        String ZIP = "zip";
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

    /**
     * 组件code
     */
    interface PLUGIN_CODE {
    }
}
