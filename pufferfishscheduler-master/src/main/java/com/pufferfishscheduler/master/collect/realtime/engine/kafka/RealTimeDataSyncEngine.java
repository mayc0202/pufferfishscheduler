package com.pufferfishscheduler.master.collect.realtime.engine.kafka;

import java.util.*;
import java.util.concurrent.*;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.data.RealTimeSyncTaskData;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.data.RealTimeSyncTaskDatas;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.RealTimeSyncTaskStatus;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;

import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.DataSyncTask;
import com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity.RuntimeConfig;

import lombok.extern.slf4j.Slf4j;
import org.sourcelab.kafka.connect.apiclient.request.dto.ConnectorStatus;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;
import org.sourcelab.kafka.connect.apiclient.rest.exceptions.ResourceNotFoundException;

/**
 * 实时数据同步引擎
 *
 * @author Mayc
 * @since 2026-03-13 17:44
 */
@Slf4j
public class RealTimeDataSyncEngine {

    /**
     * Kafka Connect 请求超时时间（秒）
     */
    private static final int REQUEST_TIMEOUT_SECONDS = 5;
    private static final long TOPIC_WAIT_TIMEOUT_MS = 60_000L;
    private static final long TOPIC_WAIT_INTERVAL_MS = 1_000L;
    private static final long STOP_STEP_TIMEOUT_SECONDS = 10L;

    /**
     * Kafka brokers地址,多个地址用逗号分隔
     */
    private final String brokers;

    /**
     * Kafka连接客户端
     */
    private final KafkaConnectClient client;

    public RealTimeDataSyncEngine(String brokers, String address) {
        // 设置Kafka brokers地址
        this.brokers = brokers;

        // 创建Kafka连接配置并设置超时时间
        Configuration configuration = new Configuration(address);
        configuration.useRequestTimeoutInSeconds(REQUEST_TIMEOUT_SECONDS);

        // 创建Kafka连接客户端
        this.client = new KafkaConnectClient(configuration);
    }

    /**
     * 启动数据同步任务
     *
     * @param task 数据同步任务
     * @return 运行时配置
     */
    public RuntimeConfig start(DataSyncTask task) {
        log.info(String.format("开始启动实时同步任务【任务号：%s】...", task.getTaskId()));
        RuntimeConfig runtimeConfig = new RuntimeConfig(null);

        // 校验入参的合法性
        task.valid();
        task.setBrokers(brokers);

        try {

            // 1.为了保持接口幂等，在创建实时同步任务前，先清理相关的连接器配置、统计任务等
            deleteConnectors(task);
            // 给 Kafka Connect 留出 1 秒钟清理元数据的时间，防止 409 Conflict
            Thread.sleep(1000);

            // 2.先清理目标表数据
            log.info("开始清理【任务号：{}】目标表数据。", task.getTaskId());
            task.getTargetDatabase().truncateTables(task.getTargetTableNames());
            log.info("完成清理【任务号：{}】目标表数据。目标表：{}", task.getTaskId(), task.getTargetTableNames());

            // 3.创建 源 kafka connect
            log.info("开始创建【任务号：{}】Kafka 源连接器。", task.getTaskId());
            // 确保历史 Topic 存在
            ensureHistoryTopicExists(task);
            String sourceConnectorName = createSourceConnector(task);
            runtimeConfig.setSourceConnectorName(sourceConnectorName);
            // 轮询 status(task) 依赖 task.runtimeConfig，创建后立即回填，避免状态判断看不到连接器名
            task.setRuntimeConfig(JSONObject.toJSONString(runtimeConfig));
            log.info("完成创建【任务号：{}】Kafka 源连接器。连接器名称：{}", task.getTaskId(), sourceConnectorName);

            // 4.创建 目标 kafka connect
            log.info("开始创建【任务号：{}】Kafka 目标连接器。", task.getTaskId());
            List<String> targetConnectorName = createTargetConnector(task);
            runtimeConfig.setTargetConnectorNames(targetConnectorName);
            // 回填目标连接器名称，避免 status() 误判“目标连接器名称未就绪”
            task.setRuntimeConfig(JSONObject.toJSONString(runtimeConfig));
            log.info("完成创建【任务号：{}】Kafka 目标连接器。连接器名称：{}", task.getTaskId(), targetConnectorName);

            // 5.等待 topic 出现后再启动统计任务（避免 UNKNOWN_TOPIC_OR_PARTITION 导致误判/噪音）
            waitForTopics(task.getTopics());

            // 6.创建统计任务
            log.info("开始创建【任务号：{}】统计任务。", task.getTaskId());
            RealTimeDataSyncStatsTask syncStatsTask = new RealTimeDataSyncStatsTask(brokers, task);

            RealTimeSyncTaskData realTimeSyncTaskData = new RealTimeSyncTaskData(task.getTaskId());
            realTimeSyncTaskData.setTaskId(task.getTaskId());
            realTimeSyncTaskData.setRealTimeDataSyncStatsTask(syncStatsTask);
            RealTimeSyncTaskDatas.register(realTimeSyncTaskData);

            syncStatsTask.start();
            log.info("完成创建【任务号：{}】统计任务。统计任务名称：{}", task.getTaskId(), syncStatsTask.getStatsName(task.getTaskId()));

            runtimeConfig.setServerName(task.getSourceDatabase().getServerName(task.getTaskId()));
            runtimeConfig.setStatsTaskName(syncStatsTask.getStatsName(task.getTaskId()));
            runtimeConfig.setTopics(task.getTopics());
            // 轮询前确保 task 上携带完整 runtimeConfig
            task.setRuntimeConfig(JSONObject.toJSONString(runtimeConfig));

            // 在 RealTimeDataSyncEngine.java 的 start 方法末尾返回前添加：
            log.info("正在验证连接器状态...");
            boolean isStarted = false;
            RealTimeSyncTaskStatus lastStatus = null;
            // 轮询 30 次，每次 2 秒钟（总计约 60 秒），避免 Connect 任务分配抖动导致误判失败
            for (int i = 0; i < 30; i++) {
                RealTimeSyncTaskStatus currentStatus = this.status(task);
                lastStatus = currentStatus;
                if (RealTimeSyncTaskStatus.TASK_STATUS_RUNNING.equals(currentStatus.getStatus())) {
                    isStarted = true;
                    break;
                } else if (RealTimeSyncTaskStatus.TASK_STATUS_FAILURE.equals(currentStatus.getStatus())) {
                    // 如果 Connect 明确报错了（如 History Topic Missing），直接抛出异常
                    throw new RuntimeException("连接器启动失败: " + currentStatus.getMessage());
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                }
            }

            if (!isStarted) {
                String last = (lastStatus == null)
                        ? "无"
                        : (lastStatus.getStatus() + (org.apache.commons.lang3.StringUtils.isBlank(lastStatus.getMessage())
                        ? "" : ("，" + lastStatus.getMessage())));
                throw new RuntimeException("连接器在 60 秒内未进入运行状态，请检查 Kafka Connect 日志。最后状态：" + last);
            }

            log.info("实时同步任务【任务号：{}】启动成功!", task.getTaskId());
            return runtimeConfig;
        } catch (Exception e) {
            // 这里不能吞掉异常，否则上层无法感知失败、无法写入 reason
            log.error("启动实时同步任务失败，任务号：{}，原因：{}", task.getTaskId(), e.getMessage(), e);
            // 创建实时同步任务失败后：只清理目标连接器 + 停止统计线程
            // 不删除源连接器，便于你通过 Connect REST/日志直接定位 Debezium 的根因。
            try {
                deleteTargetConnector(task);
            } catch (Exception ignored) {
                // ignore
            }
            RealTimeSyncTaskDatas.unRegister(task.getTaskId());
            throw new RuntimeException("启动实时同步任务失败，任务号：" + task.getTaskId() + "，原因：" + e.getMessage(), e);
        }
    }

    /**
     * 等待 Kafka Topic 出现
     *
     * @param topics Kafka Topic 名称列表
     */
    private void waitForTopics(List<String> topics) {
        if (CollectionUtils.isEmpty(topics)) {
            return;
        }

        long deadline = System.currentTimeMillis() + TOPIC_WAIT_TIMEOUT_MS;

        java.util.Properties props = new java.util.Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        try (AdminClient admin = AdminClient.create(props)) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    java.util.Set<String> existing = admin
                            .listTopics(new ListTopicsOptions().listInternal(false))
                            .names()
                            .get();

                    boolean allPresent = true;
                    for (String t : topics) {
                        if (StringUtils.isBlank(t) || !existing.contains(t)) {
                            allPresent = false;
                            break;
                        }
                    }

                    if (allPresent) {
                        log.info("Kafka topics ready: {}", topics);
                        return;
                    }
                } catch (Exception ignore) {
                    // 忽略查询异常，继续重试
                }

                try {
                    Thread.sleep(TOPIC_WAIT_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        log.warn("Kafka topics not ready within {}ms, topics={}", TOPIC_WAIT_TIMEOUT_MS, topics);
    }

    /**
     * 停止数据同步任务
     *
     * @param task 数据同步任务
     */
    public void stop(DataSyncTask task) {
        log.info("开始停止实时同步任务【任务号：{}】...", task.getTaskId());
        StringBuilder stopErrors = new StringBuilder();
        executeStopStep("删除源连接器", () -> deleteSourceConnector(task), stopErrors, true);
        executeStopStep("删除目标连接器", () -> deleteTargetConnector(task), stopErrors, true);
        // 统计任务在 close consumer 时可能略慢于 connector 删除；此步骤超时按告警处理，不阻断停止成功
        executeStopStep("停止统计任务", () -> RealTimeSyncTaskDatas.unRegister(task.getTaskId()), stopErrors, false);
        if (stopErrors.length() > 0) {
            throw new RuntimeException("停止流程存在异常: " + stopErrors);
        }
        log.info("完成停止实时同步任务【任务号：{}】。", task.getTaskId());
    }

    private void executeStopStep(String stepName, Runnable step, StringBuilder stopErrors, boolean strict) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(step);
        try {
            future.get(STOP_STEP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("{}完成", stepName);
        } catch (TimeoutException e) {
            if (strict) {
                log.error("{}超时（{}秒）", stepName, STOP_STEP_TIMEOUT_SECONDS, e);
                stopErrors.append(stepName).append("超时; ");
            } else {
                log.warn("{}超时（{}秒），按非阻断处理", stepName, STOP_STEP_TIMEOUT_SECONDS);
            }
            future.cancel(true);
        } catch (Exception e) {
            if (strict) {
                log.error("{}异常: {}", stepName, e.getMessage(), e);
                stopErrors.append(stepName).append("异常: ")
                        .append(StringUtils.defaultIfBlank(e.getMessage(), e.toString()))
                        .append("; ");
            } else {
                log.warn("{}异常（按非阻断处理）: {}", stepName, StringUtils.defaultIfBlank(e.getMessage(), e.toString()));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * 创建源连接器
     *
     * @param task 数据同步任务
     */
    private String createSourceConnector(DataSyncTask task) {
        NewConnectorDefinition connector = task.getSourceDatabase().buildSourceConnector(task);
        log.info("源连接器（" + connector.getName() + "）配置：" + JSONObject.toJSONString(connector.getConfig()));
        client.addConnector(connector);
        return connector.getName();
    }

    /**
     * 创建目标连接器
     *
     * @param task
     * @return
     */
    private List<String> createTargetConnector(DataSyncTask task) {
        List<String> connectNames = new LinkedList<>();
        List<NewConnectorDefinition> connectors = task.getTargetDatabase().buildTargetConnector(task);
        for (NewConnectorDefinition connector : connectors) {
            log.info("目标连接器（" + connector.getName() + "）配置：" + JSONObject.toJSONString(connector.getConfig()));
            client.addConnector(connector);
            connectNames.add(connector.getName());
        }

        return connectNames;
    }

    /**
     * 删除连接器
     *
     * @param task
     */
    private void deleteConnectors(DataSyncTask task) {
        deleteSourceConnector(task);
        deleteTargetConnector(task);
        RealTimeSyncTaskDatas.unRegister(task.getTaskId());
    }

    /**
     * 删除源连接器
     *
     * @param task
     */
    private void deleteSourceConnector(DataSyncTask task) {
        try {
            String connectorName = task.getSourceDatabase().getSourceConnectorName(task.getTaskId());
            log.info("删除源连接器: {}", connectorName);
            client.deleteConnector(connectorName);
        } catch (Exception e) {
            log.error("删除源连接器失败: {}", e.getMessage());
        }
    }

    /**
     * 删除目标连接器
     *
     * @param task
     */
    private void deleteTargetConnector(DataSyncTask task) {
        try {
            Collection<String> connectors = client.getConnectors();
            if (CollectionUtils.isEmpty(connectors)) {
                return;
            }

            for (String targetConnectorName : connectors) {
                if (targetConnectorName
                        .startsWith(task.getSourceDatabase().getTargetConnectorNamePrefix(task.getTaskId()))) {
                    client.deleteConnector(targetConnectorName);
                    log.info("删除目标连接器: {}", targetConnectorName);
                }
            }
        } catch (Exception e) {
            log.warn("删除目标连接器失败: {}", e.getMessage());
        }
    }

    /**
     * 查询数据同步任务状态
     *
     * @param task 数据同步任务
     * @return 任务状态
     */
    public RealTimeSyncTaskStatus status(DataSyncTask task) {
        RealTimeSyncTaskStatus taskStatus = new RealTimeSyncTaskStatus();
        taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_RUNNING);

        // 防御性处理 RuntimeConfig
        String configStr = task.getRuntimeConfig();
        RuntimeConfig runtimeConfig = StringUtils.isNotBlank(configStr) ? new RuntimeConfig(configStr) : new RuntimeConfig(null);

        // 如果是第一次启动，runtimeConfig 里没值，尝试手动从 task 的数据库适配器里算出来
        // 后续 checkSourceConnector 就会拿到正确的名字去 Kafka 查状态，而不会报 NPE
        if (StringUtils.isBlank(runtimeConfig.getSourceConnectorName())) {
            runtimeConfig.setSourceConnectorName(task.getSourceDatabase().getSourceConnectorName(task.getTaskId()));
        }

        // 1. 校验源连接器
        if (!checkSourceConnector(runtimeConfig, taskStatus)) {
            return taskStatus;
        }

        // 2. 校验目标连接器
        if (!checkTargetConnectors(runtimeConfig, taskStatus)) {
            return taskStatus;
        }

        // 3. 校验统计任务状态
        RealTimeSyncTaskData realTimeSyncTaskData = RealTimeSyncTaskDatas.getTask(task.getTaskId());
        if (realTimeSyncTaskData == null) {
            // 第一次校验时可能还没注册完成：应等待而不是直接判定失败
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
            taskStatus.setMessage("统计任务未就绪（未注册）");
            return taskStatus;
        }

        String statsStatus = realTimeSyncTaskData.getRealTimeDataSyncStatsTask().getStatus();
        if (StringUtils.isBlank(statsStatus)) {
            // stats 线程刚启动时，status 可能尚未被置为 RUNNING：等待后续轮询
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
            taskStatus.setMessage("统计任务未就绪");
            return taskStatus;
        }

        if (!RealTimeSyncTaskStatus.TASK_STATUS_RUNNING.equals(statsStatus)) {
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_FAILURE);
            taskStatus.setMessage("统计任务运行异常！");
            return taskStatus;
        }

        return taskStatus;
    }

    /**
     * 确保历史 Topic 存在
     *
     * @param task 数据同步任务
     */
    private void ensureHistoryTopicExists(DataSyncTask task) {
        String historyTopic = task.getSourceDatabase().getHistoryTopicName(
                task.getTaskId(),
                task.getSourceDatabase().getConnectionInfo().getDbName()
        );

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);

        try (AdminClient admin = AdminClient.create(props)) {
            log.info("检查并预创建 History Topic: {}", historyTopic);
            // Debezium 数据库历史 topic 需要允许写入（部分场景下历史写入的 record 可能 key 为空）。
            // 若 Topic 配成 compact，会触发：Compacted topic cannot accept message without key。
            // 因此这里默认使用 delete 策略以保证写入成功；历史积累可由运维策略清理。
            NewTopic newTopic = new NewTopic(historyTopic, 1, (short) 1);
            newTopic.configs(Map.of("cleanup.policy", "delete"));

            try {
                admin.createTopics(Collections.singleton(newTopic)).all().get();
                log.info("History Topic 准备就绪");
            } catch (java.util.concurrent.ExecutionException ex) {
                // topic 可能已存在：如果它此前被配置成 compact，会导致 Debezium 写入失败。
                // 不要自动 delete + recreate：当 topic 处于 "marked for deletion" 时会造成短时间内历史 topic 不可用，
                // Debezium 会报 "db history topic is missing"。
                log.debug("History Topic 已存在，跳过 delete + recreate: {}", ex.getMessage());
            }
        } catch (Exception e) {
            // 如果已存在会报 ExecutionException，这里通常记为 debug 或 info
            log.debug("History Topic 已存在或无需手动创建: {}", e.getMessage());
        }
    }

    /**
     * 校验源连接器状态
     *
     * @param runtimeConfig 运行时配置
     * @param taskStatus    任务状态
     * @return 是否校验通过
     */
    private boolean checkSourceConnector(RuntimeConfig runtimeConfig, RealTimeSyncTaskStatus taskStatus) {
        String sourceConnectorName = runtimeConfig.getSourceConnectorName();
        if (StringUtils.isEmpty(sourceConnectorName)) {
            // 启动流程中 runtimeConfig 可能尚未持久化，短时间内拿不到名称不应判定为 STOP
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
            taskStatus.setMessage("源连接器名称未就绪");
            return false;
        }

        try {
            ConnectorStatus connectorStatus = client.getConnectorStatus(sourceConnectorName);
            String state = connectorStatus.getConnector().get("state");
            List<ConnectorStatus.TaskStatus> tasks = connectorStatus.getTasks();

            // Connect 刚创建/重启 connector 的短时间内，tasks 可能为空或为 UNASSIGNED。
            // 这不应立即判定为 FAILURE，否则 start() 的轮询会立刻抛异常，任务会卡在 STARTING。
            if (tasks == null || tasks.isEmpty()) {
                taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
                taskStatus.setMessage(String.format("源连接器【%s】任务未就绪（tasks 为空）", sourceConnectorName));
                return false;
            }
            if (!Constants.KAFKA_STATUS.RUNNING.equals(state)) {
                // connector 本身未 RUNNING：多数情况下仍可能处于启动中，交由轮询继续等待；
                // 如果后续出现明确的 task FAILED/trace，会在下方转为 FAILURE。
                taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
                taskStatus.setMessage(String.format("源连接器【%s】状态未就绪：%s", sourceConnectorName, state));
                return false;
            }

            for (ConnectorStatus.TaskStatus ts : tasks) {
                String tsState = ts.getState();
                if (RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED.equals(tsState)) {
                    taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
                    taskStatus.setMessage(String.format("源连接器【%s】任务未分配（UNASSIGNED）", sourceConnectorName));
                    return false;
                }
                if (!RealTimeSyncTaskStatus.TASK_STATUS_RUNNING.equals(tsState)) {
                    taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_FAILURE);
                    taskStatus.setMessage(StringUtils.defaultIfBlank(ts.getTrace(),
                            String.format("源连接器【%s】任务状态异常：%s", sourceConnectorName, tsState)));
                    return false;
                }
            }
            return true;
        } catch (ResourceNotFoundException e) {
            // Connect 刚创建 connector 后，短时间内查询 status 可能返回 404
            // 不应直接判定为 STOP（否则 start() 会一直等不到 RUNNING 并最终超时）
            log.warn(e.getMessage());
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
            taskStatus.setMessage(String.format("源连接器【%s】未就绪（Connect 暂未查询到）", sourceConnectorName));
            return false;
        }
    }

    /**
     * 校验目标连接器状态
     *
     * @param runtimeConfig 运行时配置
     * @param taskStatus    任务状态
     * @return 是否校验通过
     */
    private boolean checkTargetConnectors(RuntimeConfig runtimeConfig, RealTimeSyncTaskStatus taskStatus) {
        // 启动流程中 runtimeConfig 可能尚未持久化，目标连接器名列表短时间内为空不应判定为 STOP
        List<String> targetConnectorNames = runtimeConfig.getTargetConnectorNames();
        if (targetConnectorNames == null || targetConnectorNames.isEmpty()) {
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
            taskStatus.setMessage("目标连接器名称未就绪");
            return false;
        }

        try {
            for (String targetConnectorName : targetConnectorNames) {
                ConnectorStatus connectorStatus = client.getConnectorStatus(targetConnectorName);
                List<ConnectorStatus.TaskStatus> tasks = connectorStatus.getTasks();

                if (tasks == null || tasks.isEmpty()) {
                    taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
                    taskStatus.setMessage(String.format("目标连接器【%s】任务未就绪（tasks 为空）", targetConnectorName));
                    return false;
                }

                for (ConnectorStatus.TaskStatus ts : tasks) {
                    String tsState = ts.getState();
                    if (RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED.equals(tsState)) {
                        taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
                        taskStatus.setMessage(String.format("目标连接器【%s】任务未分配（UNASSIGNED）", targetConnectorName));
                        return false;
                    }

                    if (!RealTimeSyncTaskStatus.TASK_STATUS_RUNNING.equals(tsState)) {
                        taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_FAILURE);
                        taskStatus.setMessage(StringUtils.defaultIfBlank(ts.getTrace(),
                                String.format("目标连接器【%s】任务状态异常：%s", targetConnectorName, tsState)));
                        return false;
                    }
                }
            }
            return true;
        } catch (ResourceNotFoundException e) {
            log.warn(e.getMessage());
            taskStatus.setStatus(RealTimeSyncTaskStatus.TASK_STATUS_UNASSIGNED);
            taskStatus.setMessage("目标连接器未就绪（Connect 暂未查询到）");
            return false;
        }
    }

}