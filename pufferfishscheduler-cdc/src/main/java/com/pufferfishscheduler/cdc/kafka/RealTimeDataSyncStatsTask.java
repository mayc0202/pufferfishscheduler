package com.pufferfishscheduler.cdc.kafka;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.cdc.kafka.data.RealTimeSyncTaskData;
import com.pufferfishscheduler.cdc.kafka.data.RealTimeSyncTaskDatas;
import com.pufferfishscheduler.cdc.kafka.entity.DataSyncTask;
import com.pufferfishscheduler.cdc.kafka.entity.RealTimeSyncTaskStatus;
import com.pufferfishscheduler.cdc.kafka.entity.TableMapper;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.enums.SyncDataType;
import com.pufferfishscheduler.common.exception.BusinessException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 实时数据同步统计任务：只负责消费 CDC 消息，解析后把计数写入 Redis。
 * 累计量、按小时量由 {@link RealtimeStatsRedisWriter} 写入；定时同步入库由 {@link RealTimeSyncTaskJob} 负责。
 *
 * @author Mayc
 * @since 2026-03-16  09:28
 */
@Data
@Slf4j
public class RealTimeDataSyncStatsTask extends Thread {

    private static final int MAX_EXCEPTION_TIMES = 3;
    private static final long POLL_TIMEOUT_MS = 100L;
    private static final long SHUTDOWN_CHECK_INTERVAL_MS = 2000L;
    private static final long EXCEPTION_SLEEP_MS = 2000L;

    private final String kafkaBrokers;
    private final DataSyncTask taskConfig;
    private final AtomicBoolean stopFlag = new AtomicBoolean(false);
    private final AtomicBoolean finishShutDown = new AtomicBoolean(false);

    private String status;
    private KafkaConsumer<String, String> consumer;
    private final Map<String, TableMapper> tableMappers;
    private final Map<String, String> topicSuffix2TableNameMappers;
    private final RealTimeDataSyncEngineConfig config;
    private final RealtimeStatsRedisWriter redisWriter;

    public RealTimeDataSyncStatsTask(String kafkaBrokers, DataSyncTask task) {
        this.kafkaBrokers = kafkaBrokers;
        this.taskConfig = task;

        this.tableMappers = new HashMap<>();
        this.topicSuffix2TableNameMappers = new HashMap<>();

        for (TableMapper tm : task.getTableMappers()) {
            tableMappers.put(tm.getSourceTableName(), tm);
            String topicName = task.getSourceDatabase().buildTopicName(task.getTaskId(), tm.getSourceTableName());
            topicSuffix2TableNameMappers.put(topicName, tm.getSourceTableName());
        }

        this.config = PufferfishSchedulerApplicationContext.getBean(RealTimeDataSyncEngineConfig.class);
        this.redisWriter = PufferfishSchedulerApplicationContext.getBean(RealtimeStatsRedisWriter.class);
    }

    @Override
    public void run() {
        int exceptionTimes = 0;

        try {
            initializeConsumer();
            // 让外部校验方法尽快感知“统计线程已就绪”，避免刚 start() 就被判定为异常
            status = RealTimeSyncTaskStatus.TASK_STATUS_RUNNING;
            RealTimeSyncTaskData taskData = RealTimeSyncTaskDatas.getTask(taskConfig.getTaskId());

            while (shouldContinue()) {
                try {
                    processMessages(taskData);
                    exceptionTimes = 0;
                } catch (Exception e) {
                    exceptionTimes = handleProcessingException(e, exceptionTimes);
                }
            }
        } catch (Exception e) {
            log.error("Fatal error in stats task for taskId: {}", taskConfig.getTaskId(), e);
            status = RealTimeSyncTaskStatus.TASK_STATUS_FAILURE;
        } finally {
            shutdownConsumer();
        }
    }

    private void initializeConsumer() {
        Properties props = createConsumerProperties();
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(taskConfig.getTopics());
        log.info("Stats task {} started for topics: {}", getStatsName(taskConfig.getTaskId()), taskConfig.getTopics());
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaBrokers);
        props.setProperty("group.id", getStatsName(taskConfig.getTaskId()));
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.setProperty("max.poll.records", config.consumerMaxPollRecords);
        props.setProperty("max.partition.fetch.bytes", config.consumerMaxPartitionFetchBytes);
        props.setProperty("fetch.min.bytes", config.consumerFetchMinBytes);
        props.setProperty("fetch.max.wait.ms", config.consumerFetchMaxWaitMs);
        props.setProperty("compression.type", config.consumerCompressionType);

        return props;
    }

    private boolean shouldContinue() {
        return !stopFlag.get();
    }

    /**
     * 轮询消息 → 按表聚合 → 写 Redis（累计 + 按小时），不落库。
     */
    private void processMessages(RealTimeSyncTaskData taskData) {
        status = RealTimeSyncTaskStatus.TASK_STATUS_RUNNING;

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

        if (!records.isEmpty()) {
            processRecords(records);
            try {
                consumer.commitSync();
            } catch (Exception e) {
                log.warn("Stats task commit offset failed: {}", e.getMessage());
            }
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        LocalDateTime now = LocalDateTime.now();
        int syncDate = getFormatDate(now);
        int syncHour = now.getHour();

        Map<String, RecordStats> aggregatedStats = new HashMap<>();

        for (ConsumerRecord<String, String> record : records) {
            String sourceTableName = topicSuffix2TableNameMappers.get(record.topic());
            if (sourceTableName == null) {
                log.warn("Unknown topic: {}", record.topic());
                continue;
            }

            TableMapper mapper = tableMappers.get(sourceTableName);
            if (mapper == null) {
                continue;
            }

            aggregatedStats
                    .computeIfAbsent(sourceTableName, k -> new RecordStats())
                    .add(extractOperationType(record.value()), mapper.getWriteType());
        }

        Integer taskId = taskConfig.getTaskId();
        for (Map.Entry<String, RecordStats> entry : aggregatedStats.entrySet()) {
            String tableName = entry.getKey();
            RecordStats stats = entry.getValue();
            TableMapper mapper = tableMappers.get(tableName);
            if (mapper == null) {
                continue;
            }
            redisWriter.increment(taskId, mapper.getTableMapperId(), syncDate, syncHour,
                    stats.insert, stats.update, stats.delete);
        }
    }

    private String extractOperationType(String messageValue) {
        if (StringUtils.isEmpty(messageValue)) {
            return "";
        }
        try {
            JSONObject jo = JSON.parseObject(messageValue);
            return jo.getJSONObject("payload").getString("op");
        } catch (Exception e) {
            log.debug("Failed to parse message: {}", messageValue);
            return "";
        }
    }

    private static int getFormatDate(LocalDateTime time) {
        return time.getYear() * 10000 + time.getMonthValue() * 100 + time.getDayOfMonth();
    }

    private int handleProcessingException(Exception e, int exceptionTimes) {
        log.error("Error processing messages for task: {}", taskConfig.getTaskId(), e);

        try {
            Thread.sleep(EXCEPTION_SLEEP_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        int newExceptionTimes = exceptionTimes + 1;
        if (newExceptionTimes >= MAX_EXCEPTION_TIMES) {
            log.error("Task {} failed after {} exceptions", taskConfig.getTaskId(), MAX_EXCEPTION_TIMES);
            status = RealTimeSyncTaskStatus.TASK_STATUS_FAILURE;
            throw new BusinessException("Task failed after multiple exceptions: " + e.getMessage());
        }

        return newExceptionTimes;
    }

    private void shutdownConsumer() {
        if (consumer != null) {
            try {
                consumer.close();
                log.info("Consumer closed for task: {}", taskConfig.getTaskId());
            } catch (Exception e) {
                log.error("Error closing consumer for task: {}", taskConfig.getTaskId(), e);
            }
        }
    }

    public void shutdown() {
        log.info("Shutting down stats task: {}", taskConfig.getTaskId());
        stopFlag.set(true);

        while (!finishShutDown.get()) {
            try {
                Thread.sleep(SHUTDOWN_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("Stats task shutdown complete: {}", taskConfig.getTaskId());
    }

    public String getStatsName(Integer taskId) {
        return String.format("STATS_%d", taskId);
    }

    private static class RecordStats {
        long insert;
        long update;
        long delete;

        void add(String op, String writeType) {
            long ins = 0, upd = 0, del = 0;

            if (SyncDataType.c.name().equals(op) || SyncDataType.r.name().equals(op)) {
                ins = 1;
            } else if (SyncDataType.u.name().equals(op)) {
                upd = 1;
            } else if (SyncDataType.d.name().equals(op)) {
                del = 1;
            }

            if (DataSyncTask.WRITETYPE_ONLY_INSERT.equals(writeType)) {
                ins += upd;
                upd = 0;
                del = 0;
            }

            this.insert += ins;
            this.update += upd;
            this.delete += del;
        }
    }
}
