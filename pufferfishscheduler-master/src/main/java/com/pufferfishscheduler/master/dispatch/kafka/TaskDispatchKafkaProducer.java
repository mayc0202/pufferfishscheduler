package com.pufferfishscheduler.master.dispatch.kafka;

import com.alibaba.fastjson2.JSON;
import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Master -> Kafka：单向任务派发消息
 */
@Component
public class TaskDispatchKafkaProducer implements InitializingBean, DisposableBean {

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.dispatch.topic}")
    private String topic;

    @Value("${kafka.dispatch.acks:1}")
    private String acks;

    @Value("${kafka.dispatch.retries:3}")
    private String retries;

    private KafkaProducer<String, String> producer;

    /**
     * 发送任务派发消息
     *
     * @param message 任务派发消息
     */
    public void send(TaskDispatchMessage message) {
        if (message == null) {
            return;
        }
        String key = message.getTaskType() + ":" + message.getTaskId();
        String value = JSON.toJSONString(message);
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    /**
     * 初始化 Kafka 生产者
     */
    @Override
    public void afterPropertiesSet() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * 关闭 Kafka 生产者
     */
    @Override
    public void destroy() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}

