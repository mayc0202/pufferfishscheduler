package com.pufferfishscheduler.worker.dispatch;

import com.alibaba.fastjson2.JSON;
import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.MetadataTask;
import com.pufferfishscheduler.dao.mapper.MetadataTaskMapper;
import com.pufferfishscheduler.worker.metadata.service.DbSyncExecutor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker 消费 Kafka 派发消息，执行元数据同步（只写 DB；前端查询结果）
 */
@Component
public class TaskDispatchKafkaConsumer implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatchKafkaConsumer.class);

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.dispatch.topic}")
    private String topic;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.auto-offset-reset:latest}")
    private String autoOffsetReset;

    private final MetadataTaskMapper metadataTaskMapper;
    private final DbSyncExecutor dbSyncExecutor;

    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TaskDispatchKafkaConsumer(MetadataTaskMapper metadataTaskMapper,
                                       DbSyncExecutor dbSyncExecutor) {
        this.metadataTaskMapper = metadataTaskMapper;
        this.dbSyncExecutor = dbSyncExecutor;
    }

    @Override
    public void afterPropertiesSet() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        running.set(true);
        Thread t = new Thread(this::loop, "kafka-dispatch-consumer-" + groupId);
        t.setDaemon(true);
        t.start();

        log.info("Kafka dispatch consumer started: topic={}, groupId={}", topic, groupId);
    }

    private void loop() {
        while (running.get()) {
            ConsumerRecords<String, String> records;
            try {
                records = consumer.poll(Duration.ofMillis(1000));
            } catch (Exception e) {
                log.error("Kafka poll failed", e);
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                boolean ok = process(record.value());
                if (!ok) {
                    // 失败状态已落库；继续处理下一条（避免阻塞）
                }
            }

            try {
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Kafka commitSync failed", e);
            }
        }
    }

    private boolean process(String value) {
        if (value == null || value.isBlank()) return true;

        TaskDispatchMessage msg;
        try {
            msg = JSON.parseObject(value, TaskDispatchMessage.class);
        } catch (Exception e) {
            log.warn("Invalid kafka dispatch message: {}", value, e);
            return true;
        }

        if (msg.getTaskType() == null) return true;
        if (!Constants.TASK_TYPE.METADATA_TASK.equals(msg.getTaskType())) {
            return true; // 先只实现元数据任务
        }

        if (msg.getTaskId() == null || msg.getDbId() == null || msg.getScheduledTimeMillis() == null) {
            return true;
        }

        Date scheduledDate = truncateToSecond(new Date(msg.getScheduledTimeMillis()));
        // 实际开始执行时间：与实时任务类似，Worker 接管后写入真实执行时刻，供列表展示与下次 cron 参考
        Date runStart = new Date();

        // STARTING -> RUNNING（防重入）
        UpdateWrapper<MetadataTask> toRunning = new UpdateWrapper<>();
        toRunning.eq("id", msg.getTaskId())
                .eq("execute_time", scheduledDate)
                .eq("status", Constants.JOB_MANAGE_STATUS.STARTING)
                .set("status", Constants.JOB_MANAGE_STATUS.RUNNING)
                .set("execute_time", runStart)
                .set("updated_time", runStart)
                .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);

        int updated = metadataTaskMapper.update(null, toRunning);
        if (updated != 1) {
            // 已被处理/不匹配的计划时间，忽略该消息
            return true;
        }

        // 读取失败策略，用于失败后的状态落库
        MetadataTask currentTask = metadataTaskMapper.selectById(msg.getTaskId());
        String failurePolicy = currentTask != null ? currentTask.getFailurePolicy() : "1";

        try {
            dbSyncExecutor.execute(msg.getDbId());

            Date finishedAt = new Date();
            // RUNNING -> INIT（成功后回到可再次调度状态）；清空失败原因（对齐实时任务成功落库逻辑）
            UpdateWrapper<MetadataTask> toInit = new UpdateWrapper<>();
            toInit.eq("id", msg.getTaskId())
                    .eq("status", Constants.JOB_MANAGE_STATUS.RUNNING)
                    .set("status", Constants.JOB_MANAGE_STATUS.INIT)
                    .set("reason", "")
                    .set("updated_time", finishedAt)
                    .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            metadataTaskMapper.update(null, toInit);
            return true;
        } catch (Exception e) {
            log.error("metadata sync execute failed, taskId={}, dbId={}", msg.getTaskId(), msg.getDbId(), e);

            String finalStatus = "1".equals(failurePolicy)
                    ? Constants.JOB_MANAGE_STATUS.STOP
                    : Constants.JOB_MANAGE_STATUS.FAILURE;

            String reason = buildFailureReason(e);
            Date finishedAt = new Date();
            UpdateWrapper<MetadataTask> toFail = new UpdateWrapper<>();
            toFail.eq("id", msg.getTaskId())
                    .eq("status", Constants.JOB_MANAGE_STATUS.RUNNING)
                    .set("status", finalStatus)
                    .set("reason", reason)
                    .set("updated_time", finishedAt)
                    .set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            metadataTaskMapper.update(null, toFail);
            return false;
        }
    }

    /**
     * 与实时任务类似：将异常信息写入 reason，避免 message 为空时无记录。
     */
    private static String buildFailureReason(Throwable e) {
        if (e == null) {
            return "元数据同步执行失败";
        }
        String msg = e.getMessage();
        if (StringUtils.isNotBlank(msg)) {
            // reason 列为 text，仍限制长度避免极端堆栈撑爆客户端展示
            return msg.length() > 8000 ? msg.substring(0, 8000) : msg;
        }
        return e.getClass().getSimpleName();
    }

    private static Date truncateToSecond(Date date) {
        long ms = date.getTime();
        return new Date((ms / 1000) * 1000);
    }

    @Override
    public void destroy() {
        running.set(false);
        try {
            if (consumer != null) {
                consumer.wakeup();
                consumer.close();
            }
        } catch (Exception ignore) {
        }
    }
}

