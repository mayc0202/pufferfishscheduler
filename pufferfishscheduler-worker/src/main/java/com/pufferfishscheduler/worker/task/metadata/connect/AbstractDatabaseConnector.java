package com.pufferfishscheduler.worker.task.metadata.connect;

import com.pufferfishscheduler.domain.domain.TableSchema;
import com.pufferfishscheduler.domain.model.database.DatabaseConnectionInfo;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Worker 侧简化数据库连接器抽象。
 */
public abstract class AbstractDatabaseConnector {

    private String host;
    private Integer port;
    private String username;
    private String password;
    private String type;
    private String dbName;
    private String schema;
    private String driver;
    private String url;
    private String properties;
    private String extConfig;

    public abstract AbstractDatabaseConnector build();

    public abstract Connection getConnection();

    public abstract Map<String, TableSchema> getTableSchema(List<String> inTableNames, List<String> notInTableNames);

    public DatabaseConnectionInfo getDatabaseInfo() {
        DatabaseConnectionInfo info = new DatabaseConnectionInfo();
        info.setUsername(username);
        info.setPassword(password);
        info.setType(type);
        info.setDbSchema(schema);
        info.setProperties(properties);
        info.setExtConfig(extConfig);
        return info;
    }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getDriver() { return driver; }
    public void setDriver(String driver) { this.driver = driver; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getProperties() { return properties; }
    public void setProperties(String properties) { this.properties = properties; }
    public String getExtConfig() { return extConfig; }
    public void setExtConfig(String extConfig) { this.extConfig = extConfig; }
}

