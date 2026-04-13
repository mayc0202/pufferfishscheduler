package com.pufferfishscheduler.plugin.util;


public class DbDatabaseVo {
    /**
     * 自增主键
     */
    private Integer id;

    /**
     * 数据库接入名称
     */
    private String name;

    /**
     * 数据库分类：R-关系型数据库；NR-非关系数据库；
     */
    private String category;

    private String labels;

    /**
     * 数据库类型：Mysql、Oracle、SQLServer、PostgreSQL、GaussDB、Hive、Spark、Greenplum等
     */
    private String type;

    /**
     * 主机地址
     */
    private String dbHost;

    /**
     * 端口
     */
    private String dbPort;

    /**
     * 数据库名称/SID
     */
    private String dbName;

    /**
     * 模式schema
     */
    private String dbSchema;

    /**
     * 数据库用户名
     */
    private String username;

    /**
     * 数据库密码
     */
    private String password;

    /**
     * 备注
     */
    private String remark;

    private String properties;

    private String extConfig;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public String getDbPort() {
        return dbPort;
    }

    public void setDbPort(String dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbSchema() {
        return dbSchema;
    }

    public void setDbSchema(String dbSchema) {
        this.dbSchema = dbSchema;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public String getExtConfig() {
        return extConfig;
    }

    public void setExtConfig(String extConfig) {
        this.extConfig = extConfig;
    }

    public String getLabels() {
        return labels;
    }

    public void setLabels(String labels) {
        this.labels = labels;
    }
}
