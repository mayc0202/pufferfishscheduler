package com.pufferfishscheduler.master.database.connect.mq.impl;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.master.database.connect.mq.AbstractMQConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka 连接器
 */
@Slf4j
public class KafkaConnector extends AbstractMQConnector {

    private static final int ADMIN_CLIENT_TIMEOUT_SECONDS = 10;

    /**
     * 连接消息队列
     *
     * @return
     */
    @Override
    public ConResponse connect() {
        ConResponse response = new ConResponse();

        Properties properties = buildKafkaProperties();
        String kafkaAddress = buildMQAddress(this.getDbHost(), this.getDbPort());

        // 主题名称（仅用于测试连接，不需要实际订阅）
        String topic = "test-topic";

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // 尝试订阅主题（仅用于触发连接）
            consumer.subscribe(Collections.singletonList(topic));

            // 尝试获取分区信息（触发连接）
            consumer.listTopics();

            log.info("connect to kafka cluster success");

            response.setResult(true);
            response.setMsg("连接成功！");

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.setResult(false);
            response.setMsg(String.format("连接失败! 原因:%s", e.getMessage()));
            throw new BusinessException(String.format("连接失败！原因：地址【%s】不可用！", kafkaAddress));
        }

        return response;
    }

    /**
     * 获取所有主题列表
     *
     * @return 主题名称列表
     */
    @Override
    public List<String> topics() {
        Properties properties = buildAdminClientProperties();
        String kafkaAddress = buildMQAddress(this.getDbHost(), this.getDbPort());

        try (AdminClient adminClient = AdminClient.create(properties)) {
            // 创建列出主题的请求，包括内部主题
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true);

            ListTopicsResult topicsResult = adminClient.listTopics(options);

            // 获取所有主题名称，设置超时时间
            Set<String> topicNames = topicsResult.names()
                    .get(ADMIN_CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Successfully retrieved {} topics from Kafka cluster: {}", topicNames.size(), kafkaAddress);

            return new ArrayList<>(topicNames);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while listing Kafka topics", e);
            throw new BusinessException("获取Kafka主题列表被中断: " + e.getMessage());
        } catch (ExecutionException e) {
            log.error("Failed to list Kafka topics", e);
            throw new BusinessException("获取Kafka主题列表失败: " + e.getCause().getMessage());
        } catch (TimeoutException e) {
            log.error("Timeout while listing Kafka topics", e);
            throw new BusinessException("获取Kafka主题列表超时，请检查网络连接或Kafka服务状态");
        } catch (Exception e) {
            log.error("Unexpected error while listing Kafka topics", e);
            throw new BusinessException("获取Kafka主题列表失败: " + e.getMessage());
        }
    }

    /**
     * 构建Kafka消费者配置属性
     *
     * @return Properties对象
     */
    private Properties buildKafkaProperties() {
        Properties properties = new Properties();
        String kafkaAddress = buildMQAddress(this.getDbHost(), this.getDbPort());

        // 基础配置
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.setProperty(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000");
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

        // 消费者配置
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-connection-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 添加认证配置（如果有用户名密码）
        if (StringUtils.isNotBlank(this.getUsername()) && StringUtils.isNotBlank(this.getPassword())) {
            properties.setProperty("security.protocol", "SASL_PLAINTEXT");
            properties.setProperty("sasl.mechanism", "PLAIN");
            String jaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    this.getUsername(), this.getPassword()
            );
            properties.setProperty("sasl.jaas.config", jaasConfig);
        }

        // 配置自定义属性
        configureProperties(properties, getMQInfo());

        return properties;
    }

    /**
     * 构建AdminClient配置属性
     *
     * @return Properties对象
     */
    private Properties buildAdminClientProperties() {
        Properties properties = new Properties();
        String kafkaAddress = buildMQAddress(this.getDbHost(), this.getDbPort());

        // AdminClient基础配置
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.setProperty(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000");
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

        // 添加认证配置（如果有用户名密码）
        if (StringUtils.isNotBlank(this.getUsername()) && StringUtils.isNotBlank(this.getPassword())) {
            properties.setProperty("security.protocol", "SASL_PLAINTEXT");
            properties.setProperty("sasl.mechanism", "PLAIN");
            String jaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    this.getUsername(), this.getPassword()
            );
            properties.setProperty("sasl.jaas.config", jaasConfig);
        }

        // 配置自定义属性
        configureProperties(properties, getMQInfo());

        return properties;
    }
}