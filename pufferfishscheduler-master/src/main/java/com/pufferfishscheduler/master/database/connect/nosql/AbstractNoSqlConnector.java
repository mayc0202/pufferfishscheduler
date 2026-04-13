package com.pufferfishscheduler.master.database.connect.nosql;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.result.ConResponse;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Properties;

/**
 * 非关系型数据库连接器抽象类
 * 提供Redis、MongoDB等NoSQL数据库的统一接口
 */
@Data
@Slf4j
public abstract class AbstractNoSqlConnector {

    /**
     * 数据源id
     */
    private Integer id;

    /**
     * 数据源名称
     */
    private String name;

    /**
     * 数据源类型
     */
    private String type;

    /**
     * 主机地址（支持集群，逗号分隔）
     */
    private String dbHost;

    /**
     * 端口
     */
    private String dbPort;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 数据库名称
     */
    private String database;

    /**
     * 数据库索引
     */
    private Integer databaseIndex;

    /**
     * 属性（JSON格式的自定义配置）
     */
    private String properties;

    /**
     * 测试连接
     *
     * @return 连接结果
     */
    public abstract ConResponse connect();

    /**
     * 获取所有键/集合列表
     *
     * @return 键名/集合名列表
     */
    public abstract List<String> listKeys();

    /**
     * 获取键/集合的数量
     *
     * @return 数量
     */
    public abstract long countKeys();

    /**
     * 关闭连接
     */
    public abstract void close();

    /**
     * 解析自定义属性
     *
     * @return Properties对象
     */
    protected Properties parseProperties() {
        if (org.apache.commons.lang3.StringUtils.isBlank(this.getProperties())) {
            return new Properties();
        }

        try {
            JSONObject proObj = JSONObject.parseObject(this.getProperties());
            Properties properties = new Properties();
            for (String key : proObj.keySet()) {
                properties.put(key, proObj.getString(key));
            }
            return properties;
        } catch (Exception e) {
            log.warn("Failed to parse properties: {}", e.getMessage());
            return new Properties();
        }
    }

    /**
     * 构建连接地址描述
     */
    protected String buildAddressDescription() {
        String host = this.getDbHost();
        String port = this.getDbPort();
        if (org.apache.commons.lang3.StringUtils.isBlank(port)) {
            return host;
        }
        return host + ":" + port;
    }
}