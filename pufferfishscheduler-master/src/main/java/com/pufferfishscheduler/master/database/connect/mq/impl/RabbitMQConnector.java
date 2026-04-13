package com.pufferfishscheduler.master.database.connect.mq.impl;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.common.utils.CommonUtil;
import com.pufferfishscheduler.master.database.connect.mq.AbstractMQConnector;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ 连接器
 * 支持 RabbitMQ 的连接测试和队列管理
 * 使用官方推荐的 ConnectionFactory API
 */
@Slf4j
public class RabbitMQConnector extends AbstractMQConnector {

    private static final int DEFAULT_MANAGEMENT_PORT = 15672;
    private static final int CONNECTION_TIMEOUT_MS = 30000;
    private static final int HANDSHAKE_TIMEOUT_MS = 10000;
    private static final int SHUTDOWN_TIMEOUT_MS = 10000;
    private static final int HEARTBEAT_INTERVAL_SEC = 60;

    /**
     * 连接消息队列
     *
     * @return 连接结果
     */
    @Override
    public ConResponse connect() {
        ConResponse response = new ConResponse();
        Connection connection = null;
        Channel channel = null;

        try {
            ConnectionFactory factory = buildConnectionFactory();
            String address = buildAddressDescription();

            log.info("Attempting to connect to RabbitMQ server: {}", address);

            // 创建连接
            List<Address> addresses = buildAddressList();
            if (addresses.size() > 1) {
                // 集群模式
                connection = factory.newConnection(addresses, getClientProvidedName());
            } else {
                // 单节点模式
                connection = factory.newConnection(getClientProvidedName());
            }

            channel = connection.createChannel();

            // 测试连接：声明一个临时队列并发送测试消息
            String testQueue = "test_connection_" + CommonUtil.getUUIDString();

            // 声明临时队列（自动删除）
            channel.queueDeclare(testQueue, false, true, true, null);

            // 发送测试消息
            String testMessage = "Connection test message at " + new Date();
            channel.basicPublish("", testQueue,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    testMessage.getBytes());

            // 接收测试消息
            GetResponse getResponse = channel.basicGet(testQueue, true);
            if (getResponse != null) {
                String receivedMessage = new String(getResponse.getBody());
                log.info("Successfully verified RabbitMQ connection: sent and received test message");
            } else {
                log.warn("Test message not received, but connection is established");
            }

            log.info("Successfully connected to RabbitMQ server: {}", address);

            response.setResult(true);
            response.setMsg("连接成功！");

        } catch (Exception e) {
            log.error("Failed to connect to RabbitMQ server", e);
            response.setResult(false);
            response.setMsg(String.format("连接失败! 原因:%s", e.getMessage()));
            throw new BusinessException(String.format("连接失败！原因：%s不可用！",
                    buildAddressDescription()));
        } finally {
            // 关闭资源
            closeQuietly(channel);
            closeQuietly(connection);
        }

        return response;
    }

    /**
     * 获取所有队列列表
     * 注意：RabbitMQ中队列（Queue）对应Kafka中的主题（Topic）
     * 通过Management HTTP API获取所有队列（需要启用management插件）
     *
     * @return 队列名称列表
     */
    @Override
    public List<String> topics() {
        try {
            // 通过Management API获取所有队列
            List<String> queues = getQueuesViaManagementAPI();

            if (!queues.isEmpty()) {
                log.info("Successfully retrieved {} queues via Management API", queues.size());
                return queues;
            }

            // 如果Management API不可用，返回空列表并记录警告
            log.warn("Unable to retrieve queues via Management API. Please ensure RabbitMQ Management plugin is enabled.");
            return Collections.emptyList();

        } catch (Exception e) {
            log.error("Failed to list RabbitMQ queues", e);
            throw new BusinessException("获取RabbitMQ队列列表失败: " + e.getMessage());
        }
    }

    /**
     * 构建连接工厂
     * 使用官方推荐的配置方式
     *
     * @return ConnectionFactory对象
     */
    private ConnectionFactory buildConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();

        // 设置认证信息
        if (StringUtils.isNotBlank(this.getUsername())) {
            factory.setUsername(this.getUsername());
        }
        if (StringUtils.isNotBlank(this.getPassword())) {
            factory.setPassword(this.getPassword());
        }

        // 设置虚拟主机（默认"/"）
        String virtualHost = StringUtils.isNotBlank(this.getTopic()) ? this.getTopic() : "/";
        factory.setVirtualHost(virtualHost);

        // 设置连接参数
        factory.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
        factory.setHandshakeTimeout(HANDSHAKE_TIMEOUT_MS);
        factory.setShutdownTimeout(SHUTDOWN_TIMEOUT_MS);
        factory.setRequestedHeartbeat(HEARTBEAT_INTERVAL_SEC);

        // 设置自动恢复（生产环境建议开启）
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // 5秒重试间隔

        // 配置单节点主机和端口
        configureSingleNode(factory);

        // 配置SSL（如果需要）
        configureSSL(factory);

        // 配置自定义属性
        applyCustomProperties(factory);

        return factory;
    }

    /**
     * 构建地址列表（用于集群连接）
     */
    private List<Address> buildAddressList() {
        String host = this.getDbHost();
        String port = this.getDbPort();

        if (StringUtils.isBlank(host)) {
            throw new BusinessException("RabbitMQ主机地址不能为空");
        }

        List<Address> addresses = new ArrayList<>();

        // 处理集群模式（多个地址用逗号分隔）
        if (host.contains(",")) {
            for (String h : host.split(",")) {
                addresses.add(parseAddress(h.trim(), port));
            }
        } else {
            // 单节点模式
            if (host.contains(":")) {
                addresses.add(parseAddress(host, port));
            } else {
                int finalPort = StringUtils.isNotBlank(port) ? Integer.parseInt(port) :
                        (isSSLConfigured() ? ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT : ConnectionFactory.DEFAULT_AMQP_PORT);
                addresses.add(new Address(host, finalPort));
            }
        }

        return addresses;
    }

    /**
     * 配置单节点主机和端口
     */
    private void configureSingleNode(ConnectionFactory factory) {
        String host = this.getDbHost();
        String port = this.getDbPort();

        // 如果不是集群模式，设置单节点配置
        if (host != null && !host.contains(",")) {
            factory.setHost(host);
            if (StringUtils.isNotBlank(port)) {
                factory.setPort(Integer.parseInt(port));
            }
        }
    }

    /**
     * 解析地址字符串
     */
    private Address parseAddress(String host, String defaultPort) {
        if (host.contains(":")) {
            String[] parts = host.split(":");
            return new Address(parts[0], Integer.parseInt(parts[1]));
        } else if (StringUtils.isNotBlank(defaultPort)) {
            return new Address(host, Integer.parseInt(defaultPort));
        } else {
            // 判断是否使用SSL来决定默认端口
            boolean isSSL = isSSLConfigured();
            int defaultAmqpPort = isSSL ?
                    ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT :
                    ConnectionFactory.DEFAULT_AMQP_PORT;
            return new Address(host, defaultAmqpPort);
        }
    }

    /**
     * 配置SSL
     */
    private void configureSSL(ConnectionFactory factory) {
        Properties props = parseProperties();
        if (props != null && "true".equals(props.getProperty("ssl.enabled"))) {
            try {
                // 使用TLSv1.2协议（官方推荐）
                factory.useSslProtocol();

                // 可选：启用主机名验证
                if ("true".equals(props.getProperty("ssl.hostname.verification"))) {
                    factory.enableHostnameVerification();
                }

                log.info("SSL/TLS enabled for RabbitMQ connection");
            } catch (Exception e) {
                log.error("Failed to configure SSL", e);
                throw new BusinessException("RabbitMQ SSL配置失败: " + e.getMessage());
            }
        }
    }

    /**
     * 判断是否配置了SSL
     */
    private boolean isSSLConfigured() {
        Properties props = parseProperties();
        return props != null && "true".equals(props.getProperty("ssl.enabled"));
    }

    /**
     * 应用自定义属性到连接工厂
     */
    private void applyCustomProperties(ConnectionFactory factory) {
        Properties props = parseProperties();
        if (props == null || props.isEmpty()) {
            return;
        }

        // 设置客户端连接名称
        if (props.containsKey("connection.name")) {
            Map<String, Object> clientProps = new HashMap<>();
            clientProps.put("connection_name", props.getProperty("connection.name"));
            factory.setClientProperties(clientProps);
        }

        // 设置通道最大数
        if (props.containsKey("channel.max")) {
            try {
                factory.setRequestedChannelMax(Integer.parseInt(props.getProperty("channel.max")));
            } catch (NumberFormatException e) {
                log.warn("Invalid channel.max value: {}", props.getProperty("channel.max"));
            }
        }

        // 设置帧最大大小
        if (props.containsKey("frame.max")) {
            try {
                factory.setRequestedFrameMax(Integer.parseInt(props.getProperty("frame.max")));
            } catch (NumberFormatException e) {
                log.warn("Invalid frame.max value: {}", props.getProperty("frame.max"));
            }
        }

        // 设置心跳间隔
        if (props.containsKey("heartbeat")) {
            try {
                factory.setRequestedHeartbeat(Integer.parseInt(props.getProperty("heartbeat")));
            } catch (NumberFormatException e) {
                log.warn("Invalid heartbeat value: {}", props.getProperty("heartbeat"));
            }
        }

        // 设置连接超时
        if (props.containsKey("connection.timeout")) {
            try {
                factory.setConnectionTimeout(Integer.parseInt(props.getProperty("connection.timeout")));
            } catch (NumberFormatException e) {
                log.warn("Invalid connection.timeout value: {}", props.getProperty("connection.timeout"));
            }
        }

        // 设置网络恢复间隔
        if (props.containsKey("network.recovery.interval")) {
            try {
                factory.setNetworkRecoveryInterval(Long.parseLong(props.getProperty("network.recovery.interval")));
            } catch (NumberFormatException e) {
                log.warn("Invalid network.recovery.interval value: {}", props.getProperty("network.recovery.interval"));
            }
        }
    }

    /**
     * 通过Management API获取队列列表
     * 需要启用RabbitMQ Management插件
     */
    private List<String> getQueuesViaManagementAPI() {
        List<String> queueNames = new ArrayList<>();

        try {
            String managementHost = extractManagementHost();
            String managementPort = extractManagementPort();
            String virtualHost = StringUtils.isNotBlank(this.getTopic()) ? this.getTopic() : "/";

            // 构建Management API URL
            String apiUrl = String.format("http://%s:%s/api/queues/%s",
                    managementHost, managementPort,
                    encodeVirtualHost(virtualHost));

            log.debug("Calling RabbitMQ Management API: {}", apiUrl);

            // 使用Java HTTP Client调用API
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(10))
                    .build();

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create(apiUrl))
                    .header("Authorization", getBasicAuthHeader())
                    .timeout(java.time.Duration.ofSeconds(30))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            java.net.http.HttpResponse<String> response = client.send(request,
                    java.net.http.HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                // 解析JSON响应
                com.alibaba.fastjson2.JSONArray queues = com.alibaba.fastjson2.JSONArray.parseArray(response.body());
                for (int i = 0; i < queues.size(); i++) {
                    com.alibaba.fastjson2.JSONObject queue = queues.getJSONObject(i);
                    String queueName = queue.getString("name");
                    if (StringUtils.isNotBlank(queueName) && !queueName.startsWith("amq.")) {
                        // 过滤掉系统队列
                        queueNames.add(queueName);
                    }
                }
                log.info("Retrieved {} queues via Management API", queueNames.size());
            } else {
                log.warn("Management API returned status code: {}", response.statusCode());
            }

        } catch (Exception e) {
            log.warn("Failed to get queues via Management API: {}", e.getMessage());
        }

        return queueNames;
    }

    /**
     * 提取Management API的主机地址
     */
    private String extractManagementHost() {
        String host = this.getDbHost();
        if (host != null && host.contains(",")) {
            // 如果是集群，使用第一个节点
            String firstHost = host.split(",")[0].trim();
            if (firstHost.contains(":")) {
                return firstHost.split(":")[0];
            }
            return firstHost;
        } else if (host != null && host.contains(":")) {
            return host.split(":")[0];
        }
        return host != null ? host : "localhost";
    }

    /**
     * 提取Management API的端口
     */
    private String extractManagementPort() {
        Properties props = parseProperties();
        if (props != null && props.containsKey("management.port")) {
            return props.getProperty("management.port");
        }
        return String.valueOf(DEFAULT_MANAGEMENT_PORT);
    }

    /**
     * 编码虚拟主机路径
     */
    private String encodeVirtualHost(String virtualHost) {
        if (virtualHost == null || "/".equals(virtualHost)) {
            return "%2F";
        }
        try {
            return java.net.URLEncoder.encode(virtualHost, "UTF-8");
        } catch (Exception e) {
            return virtualHost;
        }
    }

    /**
     * 解析自定义属性
     */
    private Properties parseProperties() {
        if (StringUtils.isBlank(this.getProperties())) {
            return new Properties();
        }

        try {
            com.alibaba.fastjson2.JSONObject proObj =
                    com.alibaba.fastjson2.JSONObject.parseObject(this.getProperties());
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
     * 获取Basic Auth头信息
     */
    private String getBasicAuthHeader() {
        String username = StringUtils.isNotBlank(this.getUsername()) ? this.getUsername() : "guest";
        String password = StringUtils.isNotBlank(this.getPassword()) ? this.getPassword() : "guest";
        String auth = username + ":" + password;
        String encodedAuth = java.util.Base64.getEncoder().encodeToString(auth.getBytes());
        return "Basic " + encodedAuth;
    }

    /**
     * 获取客户端提供的连接名称
     */
    private String getClientProvidedName() {
        return String.format("%s-%s-%d",
                StringUtils.isNotBlank(this.getName()) ? this.getName() : "rabbitmq-connection",
                this.getClientId() != null ? this.getClientId() : "default",
                System.currentTimeMillis());
    }

    /**
     * 构建地址描述（用于日志）
     */
    private String buildAddressDescription() {
        String host = this.getDbHost();
        String port = this.getDbPort();
        if (StringUtils.isBlank(port)) {
            return host;
        }
        return host + ":" + port;
    }

    /**
     * 关闭Channel（静默关闭）
     */
    private void closeQuietly(Channel channel) {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException | TimeoutException e) {
                log.debug("Error closing channel: {}", e.getMessage());
            }
        }
    }

    /**
     * 关闭Connection（静默关闭）
     */
    private void closeQuietly(Connection connection) {
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
                log.debug("Error closing connection: {}", e.getMessage());
            }
        }
    }
}