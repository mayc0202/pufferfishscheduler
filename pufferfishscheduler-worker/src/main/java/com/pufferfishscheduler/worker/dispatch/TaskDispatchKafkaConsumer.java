package com.pufferfishscheduler.worker.dispatch;

import com.alibaba.fastjson2.JSON;
import com.pufferfishscheduler.api.dispatch.TaskDispatchMessage;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.MetadataTask;
import com.pufferfishscheduler.dao.entity.TransTask;
import com.pufferfishscheduler.dao.mapper.MetadataTaskMapper;
import com.pufferfishscheduler.dao.mapper.TransTaskMapper;
import com.pufferfishscheduler.worker.task.metadata.service.DbSyncExecutor;
import com.pufferfishscheduler.worker.task.trans.service.TransTaskExecutor;
import lombok.AllArgsConstructor;
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
 * Worker 消费 Kafka 派发消息：元数据同步、转换任务（只写 DB；前端查询结果）
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

    @Value("${kafka.consumer.max-poll-interval-ms:900000}")
    private int maxPollIntervalMs;

    /**
     * 元数据任务映射器
     */
    private final MetadataTaskMapper metadataTaskMapper;
    /**
     * 数据库同步执行器
     */
    private final DbSyncExecutor dbSyncExecutor;
    /**
     * 转换任务映射器
     */
    private final TransTaskMapper transTaskMapper;
    /**
     * 转换任务执行器
     */
    private final TransTaskExecutor transTaskExecutor;

    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TaskDispatchKafkaConsumer(MetadataTaskMapper metadataTaskMapper, DbSyncExecutor dbSyncExecutor, TransTaskMapper transTaskMapper, TransTaskExecutor transTaskExecutor) {
        this.metadataTaskMapper = metadataTaskMapper;
        this.dbSyncExecutor = dbSyncExecutor;
        this.transTaskMapper = transTaskMapper;
        this.transTaskExecutor = transTaskExecutor;
    }

    /**
     * 初始化 Kafka 消费者
     */
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
        // 转换可能长时间运行，避免超过 max.poll.interval 被踢出消费组（可按需调大）
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        running.set(true);
        Thread t = new Thread(this::loop, "kafka-dispatch-consumer-" + groupId);
        t.setDaemon(true);
        t.start();

        log.info("Kafka dispatch consumer started: topic={}, groupId={}", topic, groupId);
    }

    /**
     * 消费循环
     */
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

    /**
     * 处理 Kafka 消息
     */
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

        // 元数据任务
        if (Constants.TASK_TYPE.METADATA_TASK.equals(msg.getTaskType())) {
            return processMetadataTask(msg);
        }
        // 转换任务
        if (Constants.TASK_TYPE.TRANS_TASK.equals(msg.getTaskType())) {
            return processTransTask(msg);
        }
        // 转换任务停止
        if (Constants.TASK_TYPE.TRANS_TASK_STOP.equals(msg.getTaskType())) {
            return processTransTaskStop(msg);
        }
        return true;
    }

    /**
     * 处理元数据任务
     */
    private boolean processMetadataTask(TaskDispatchMessage msg) {
        if (msg.getTaskId() == null || msg.getDbId() == null || msg.getScheduledTimeMillis() == null) {
            return true;
        }

        Date scheduledDate = truncateToSecond(new Date(msg.getScheduledTimeMillis()));
        Date runStart = new Date();

        UpdateWrapper<MetadataTask> toRunning = new UpdateWrapper<>();
        toRunning.eq("id", msg.getTaskId()).eq("execute_time", scheduledDate).eq("status", Constants.JOB_MANAGE_STATUS.STARTING).set("status", Constants.JOB_MANAGE_STATUS.RUNNING).set("execute_time", runStart).set("updated_time", runStart).set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);

        int updated = metadataTaskMapper.update(null, toRunning);
        if (updated != 1) {
            return true;
        }

        MetadataTask currentTask = metadataTaskMapper.selectById(msg.getTaskId());
        String failurePolicy = currentTask != null ? currentTask.getFailurePolicy() : "1";

        try {
            dbSyncExecutor.execute(msg.getDbId());

            Date finishedAt = new Date();
            UpdateWrapper<MetadataTask> toInit = new UpdateWrapper<>();
            toInit.eq("id", msg.getTaskId()).eq("status", Constants.JOB_MANAGE_STATUS.RUNNING).set("status", Constants.JOB_MANAGE_STATUS.INIT).set("reason", "").set("updated_time", finishedAt).set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            metadataTaskMapper.update(null, toInit);
            return true;
        } catch (Exception e) {
            log.error("metadata sync execute failed, taskId={}, dbId={}", msg.getTaskId(), msg.getDbId(), e);

            String finalStatus = "1".equals(failurePolicy) ? Constants.JOB_MANAGE_STATUS.STOP : Constants.JOB_MANAGE_STATUS.FAILURE;

            String reason = buildFailureReason(e);
            Date finishedAt = new Date();
            UpdateWrapper<MetadataTask> toFail = new UpdateWrapper<>();
            toFail.eq("id", msg.getTaskId()).eq("status", Constants.JOB_MANAGE_STATUS.RUNNING).set("status", finalStatus).set("reason", reason).set("updated_time", finishedAt).set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);
            metadataTaskMapper.update(null, toFail);
            return false;
        }
    }

    /**
     * 转换任务：与元数据任务相同的状态机（STARTING → RUNNING → INIT / STOP|FAILURE），不要求 dbId。
     */
    private boolean processTransTask(TaskDispatchMessage msg) {
        if (msg.getTaskId() == null || msg.getScheduledTimeMillis() == null) {
            return true;
        }

        Date scheduledDate = truncateToSecond(new Date(msg.getScheduledTimeMillis()));
        Date runStart = new Date();

        UpdateWrapper<TransTask> toRunning = new UpdateWrapper<>();
        toRunning.eq("id", msg.getTaskId()).eq("execute_time", scheduledDate).eq("status", Constants.JOB_MANAGE_STATUS.STARTING).eq("deleted", Constants.DELETE_FLAG.FALSE).set("status", Constants.JOB_MANAGE_STATUS.RUNNING).set("execute_time", runStart).set("updated_time", runStart).set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);

        int updated = transTaskMapper.update(null, toRunning);
        if (updated != 1) {
            return true;
        }

        transTaskExecutor.submitExecute(msg.getTaskId());
        return true;
    }

    /**
     * 立即停止：Master 已将任务置为 STOPPING 并写入本次派发的 execute_time（秒级）。
     * 通过抢占式更新 reason 避免同一条 Kafka 消息重复触发停止逻辑。
     */
    private static final String TRANS_STOP_DISPATCH_TAG = "__trans_stop_dispatch__";

    private boolean processTransTaskStop(TaskDispatchMessage msg) {
        if (msg.getTaskId() == null || msg.getScheduledTimeMillis() == null) {
            return true;
        }

        Date scheduledDate = truncateToSecond(new Date(msg.getScheduledTimeMillis()));
        Date now = new Date();

        UpdateWrapper<TransTask> claim = new UpdateWrapper<>();
        claim.eq("id", msg.getTaskId()).eq("status", Constants.JOB_MANAGE_STATUS.STOPPING).eq("execute_time", scheduledDate).eq("deleted", Constants.DELETE_FLAG.FALSE).and(w -> w.isNull("reason").or().eq("reason", "")).set("reason", TRANS_STOP_DISPATCH_TAG).set("updated_time", now).set("updated_by", Constants.SYS_OP_INFO.SYSTEM_ACCOUNT);

        int updated = transTaskMapper.update(null, claim);
        if (updated != 1) {
            return true;
        }

        transTaskExecutor.stopExecute(msg.getTaskId());
        return true;
    }

    /**
     * 与实时任务类似：将异常信息写入 reason，避免 message 为空时无记录。
     */
    private static String buildFailureReason(Throwable e) {
        if (e == null) {
            return "任务执行失败";
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

    /**
     * 销毁 Kafka 消费者
     */
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

