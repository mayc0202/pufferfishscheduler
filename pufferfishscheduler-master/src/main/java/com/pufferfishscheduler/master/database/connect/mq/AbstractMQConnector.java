package com.pufferfishscheduler.master.database.connect.mq;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 消息队列连接器抽象类
 */
@Data
@Slf4j
public abstract class AbstractMQConnector {

    /**
     * 数据源id
     */
    private Integer id;

    /**
     * 客户端id
     */
    private String clientId;

    /**
     * 数据源名称
     */
    private String name;

    /**
     * 数据源类型
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
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 主题
     */
    private String topic;

    /**
     * 队列
     */
    private String queue;

    /**
     * 属性
     */
    private String properties;

    /**
     * 连接消息队列
     *
     * @return
     */
    public abstract ConResponse connect();

    /**
     * 获取所有主题
     *
     * @return
     */
    public abstract List<String> topics();

    /**
     * 构造MQ地址，多个主机以逗号分隔，如果主机中没有包含端口，加上端口。
     * @param host
     * @param port
     * @return
     */
    public String buildMQAddress(String host, String port){
        if(StringUtils.isBlank(host)){
            return "";
        }
        List<String> hostList = new ArrayList<>();
        String[] hostArray = host.trim().split(",");
        for (String h : hostArray) {
            if (h.contains(":") || StringUtils.isBlank(port)) {
                hostList.add(h);
            } else {
                hostList.add(h + ":" + port);
            }
        }
        return StringUtils.join(hostList, ",");
    }

    /**
     * 配置属性
     *
     * @param properties     属性集合
     * @param connectionInfo 消息队列连接信息
     */
    protected void configureProperties(Properties properties, DBConnectionInfo connectionInfo) {
        if (StringUtils.isNotBlank(connectionInfo.getProperties())) {
            JSONObject proObj = JSONObject.parseObject(connectionInfo.getProperties());
            for (String key : proObj.keySet()) {
                properties.put(key, proObj.getString(key));
            }
        }
    }

    /**
     * 获取消息队列连接信息
     *
     * @return 消息队列连接信息
     */
    public DBConnectionInfo getMQInfo() {
        DBConnectionInfo connectionInfo = new DBConnectionInfo();
        connectionInfo.setId(id);
        connectionInfo.setClientId(clientId);
        connectionInfo.setName(name);
        connectionInfo.setType(type);
        connectionInfo.setDbHost(dbHost);
        connectionInfo.setDbPort(dbPort);
//        connectionInfo.setUsername(username);
//        connectionInfo.setPassword(password);
        connectionInfo.setTopic(topic);
        connectionInfo.setQueue(queue);
        connectionInfo.setProperties(properties);
        return connectionInfo;
    }
}
