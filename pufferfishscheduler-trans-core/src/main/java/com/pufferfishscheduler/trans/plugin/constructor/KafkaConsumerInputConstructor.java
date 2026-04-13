package com.pufferfishscheduler.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.vo.collect.PropertiesVo;
import com.pufferfishscheduler.trans.engine.DataFlowRepository;
import com.pufferfishscheduler.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepContext;
import com.pufferfishscheduler.trans.runtime.TransPluginRuntime;
import com.pufferfishscheduler.plugin.KafkaConsumerField;
import com.pufferfishscheduler.plugin.KafkaConsumerInputMeta;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Kafka消费者输入组件构造器
 * 负责将JSON配置转换为KafkaConsumerInputMeta对象
 */
public class KafkaConsumerInputConstructor extends AbstractStepMetaConstructor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerInputConstructor.class);

    /** 与 {@code plugin.xml} / {@link KafkaConsumerInputMeta} 上 @Step 的 id 一致 */
    private static final String KAFKA_CONSUMER_PLUGIN_ID = "KafkaConsumerInput";

    // 配置键常量
    private static final String KEY_NAME = "name";
    private static final String KEY_DATA = "data";
    private static final String KEY_DATA_SOURCE = "dataSource";
    private static final String KEY_TOPICS = "topics";
    private static final String KEY_CONSUMER_GROUP = "consumerGroup";
    private static final String KEY_BATCH_DURATION = "batchDuration";
    private static final String KEY_BATCH_SIZE = "batchSize";
    private static final String KEY_PREFETCH_COUNT = "prefetchCount";
    private static final String KEY_PARTITION = "partition";
    private static final String KEY_COMMIT_TYPE = "commitType";
    private static final String KEY_FIELD_LIST = "fieldList";
    private static final String KEY_PROPERTIES = "properties";
    private static final String KEY_COPIES_CACHE = "copiesCache";
    private static final String KEY_DISTRIBUTE_TYPE = "distributeType";
    private static final String KEY_SUB_TRANSFORMATION_PATH = "subTransformationPath";
    private static final String KEY_SUB_STEP = "subStep";
    private static final String TRANS_ID = "transId";

    // 字段属性常量
    private static final String FIELD_KAFKA_NAME = "kafkaName";
    private static final String FIELD_OUTPUT_NAME = "outputName";
    private static final String FIELD_OUTPUT_TYPE = "outputType";

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        validateInput(config, context);

        JSONObject jsonObject = parseConfig(config);
        String componentName = extractComponentName(jsonObject);
        JSONObject data = extractData(jsonObject);

        boolean validateMode = context.isValidate();

        KafkaConsumerInputMeta stepMeta = buildStepMeta(data, componentName, validateMode, context.getFlowId());
        StepMeta targetStepMeta = buildStepMeta(context, stepMeta, componentName, data);

        applyStepConfiguration(targetStepMeta, data);

        LOGGER.debug("Kafka Consumer Input step created successfully: {}", componentName);
        return targetStepMeta;
    }

    /**
     * 解析JSON配置
     */
    private JSONObject parseConfig(String config) {
        JSONObject jsonObject = JSONObject.parseObject(config);
        if (jsonObject == null) {
            throw new BusinessException("组件数据不能为空！");
        }
        return jsonObject;
    }

    /**
     * 提取组件名称
     */
    private String extractComponentName(JSONObject jsonObject) {
        String name = jsonObject.getString(KEY_NAME);
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }
        return name;
    }

    /**
     * 提取数据部分
     */
    private JSONObject extractData(JSONObject jsonObject) {
        JSONObject data = jsonObject.getJSONObject(KEY_DATA);
        if (data == null) {
            throw new BusinessException("组件数据不能为空！");
        }
        return data;
    }

    /**
     * 构建步骤元数据
     */
    private KafkaConsumerInputMeta buildStepMeta(JSONObject data, String componentName, boolean validateMode, Integer flowId) {
        KafkaConsumerInputMeta stepMeta = new KafkaConsumerInputMeta();
        stepMeta.setDefault();

        // 获取数据源
        DbDatabase database = loadDatabase(data, componentName, validateMode);

        // 基础配置
        configureBasicSettings(stepMeta, data, database);

        // 消费配置
        configureConsumerSettings(stepMeta, data);

        // 输出字段配置
        configureOutputFields(stepMeta, data);

        // 自定义配置
        configureCustomProperties(stepMeta, data, database);

        // 子转换：Kettle BaseStreamStep + SubtransExecutor 消费批次（与 transformationPath / SUB_STEP XML 一致）
        configureSubTransformation(stepMeta, data);

        return stepMeta;
    }

    /**
     * 配置处理消息的子转换路径与入口步骤名（可选；对应 Kettle 流式步骤中的「处理消息转换」）。
     */
    private void configureSubTransformation(KafkaConsumerInputMeta stepMeta, JSONObject data) {
        Optional.ofNullable(data.getString(KEY_SUB_TRANSFORMATION_PATH))
                .filter(StringUtils::isNotBlank)
                .ifPresent(path -> {
                    stepMeta.setTransformationPath(path);
                    stepMeta.setFileName(path);
                });
        Optional.ofNullable(data.getString(KEY_SUB_STEP))
                .filter(StringUtils::isNotBlank)
                .ifPresent(stepMeta::setSubStep);

        String transId = data.getString(TRANS_ID);
        if (StringUtils.isBlank(transId)) {
            return;
        }
        String transIdTrim = transId.trim();
        try {
            TransPluginRuntime runtime = PufferfishSchedulerApplicationContext.getBean(TransPluginRuntime.class);
            String stage = runtime.getTransFlowStage(Integer.parseInt(transIdTrim));
            if (StringUtils.isBlank(stage)) {
                LOGGER.warn("未找到子转换 flowId={} 的 stage，跳过 bizType 设置", transIdTrim);
                return;
            }
            String biz = stage + "_" + Constants.TRANS;
            stepMeta.setBizType(biz);
            stepMeta.setBizObjectId(transIdTrim);
            // Kettle 的 loadMappingMeta 需要磁盘上的 KTR；不能把 transId 当路径。从 kettle_flow_repository 取子流程 XML 落盘。
            if (StringUtils.isBlank(stepMeta.getTransformationPath())) {
                String ktrPath = materializeSubTransKtrFromRepository(biz, transIdTrim);
                if (StringUtils.isNotBlank(ktrPath)) {
                    stepMeta.setTransformationPath(ktrPath);
                    stepMeta.setFileName(ktrPath);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("解析子转换 transId={} 失败: {}", transIdTrim, e.getMessage());
        }
    }

    /**
     * 将 {@code kettle_flow_repository} 中的子转换 XML 写入临时 .ktr，供 {@link org.pentaho.di.trans.StepWithMappingMeta#loadMappingMeta} 加载。
     */
    private static String materializeSubTransKtrFromRepository(String bizType, String bizObjectId) {
        if (StringUtils.isAnyBlank(bizType, bizObjectId)) {
            return null;
        }
        try {
            TransFlowConfig cfg = DataFlowRepository.getRepository().getTrans(bizType, bizObjectId);
            if (cfg == null || StringUtils.isBlank(cfg.getFlowContent())) {
                LOGGER.warn("子转换仓库无内容: bizType={}, bizObjectId={}", bizType, bizObjectId);
                return null;
            }
            String safeBiz = bizType.replaceAll("[^a-zA-Z0-9._-]", "_");
            Path dir = Paths.get(System.getProperty("java.io.tmpdir"), "pfs-kettle-subtrans");
            Files.createDirectories(dir);
            Path out = dir.resolve(safeBiz + "_" + bizObjectId + ".ktr");
            Files.writeString(out, cfg.getFlowContent(), StandardCharsets.UTF_8);
            return out.toAbsolutePath().toString();
        } catch (Exception e) {
            LOGGER.warn("落盘子转换 KTR 失败 bizType={} bizObjectId={}: {}", bizType, bizObjectId, e.getMessage());
            return null;
        }
    }

    /**
     * 加载数据源信息
     */
    private DbDatabase loadDatabase(JSONObject data, String componentName, boolean validateMode) {
        String dataSourceId = data.getString(KEY_DATA_SOURCE);

        if (validateMode) {
            validateBlank(dataSourceId, "【" + componentName + "】", "数据源");
        }

        TransPluginRuntime runtime = PufferfishSchedulerApplicationContext.getBean(TransPluginRuntime.class);
        DbDatabase database = runtime.getDatabaseById(Integer.valueOf(dataSourceId));

        if (database == null) {
            throw new BusinessException("数据源不存在!");
        }

        return database;
    }

    /**
     * 配置基础设置
     */
    private void configureBasicSettings(KafkaConsumerInputMeta stepMeta, JSONObject data, DbDatabase database) {
        // 连接方式
        stepMeta.setConnectionType(KafkaConsumerInputMeta.ConnectionType.DIRECT);

        // Kafka地址
        String bootstrapServers = buildKafkaAddress(database.getDbHost(), database.getDbPort());
        stepMeta.setDirectBootstrapServers(bootstrapServers);

        // 主题列表
        JSONArray topicsArray = data.getJSONArray(KEY_TOPICS);
        if (topicsArray == null) {
            topicsArray = new JSONArray();
        }
        List<String> topics = topicsArray.toJavaList(String.class);
        stepMeta.setTopics(topics);
    }

    /**
     * 配置消费设置
     */
    private void configureConsumerSettings(KafkaConsumerInputMeta stepMeta, JSONObject data) {
        // 消费组
        Optional.ofNullable(data.getString(KEY_CONSUMER_GROUP))
                .ifPresent(stepMeta::setConsumerGroup);

        // 批次配置
        Optional.ofNullable(data.getString(KEY_BATCH_DURATION))
                .ifPresent(stepMeta::setBatchDuration);
        Optional.ofNullable(data.getString(KEY_BATCH_SIZE))
                .ifPresent(stepMeta::setBatchSize);

        // 预取配置
        Optional.ofNullable(data.getString(KEY_PREFETCH_COUNT))
                .filter(StringUtils::isNotBlank)
                .ifPresent(stepMeta::setPrefetchCount);

        // 分区配置
        Optional.ofNullable(data.getString(KEY_PARTITION))
                .filter(StringUtils::isNotBlank)
                .ifPresent(stepMeta::setPartition);

        // 偏移量提交方式
        String commitType = data.getString(KEY_COMMIT_TYPE);
        stepMeta.setAutoCommit(Constants.COMMIT_TYPE.AUTO_COMMIT.equals(commitType));
    }

    /**
     * 配置输出字段
     */
    private void configureOutputFields(KafkaConsumerInputMeta stepMeta, JSONObject data) {
        JSONArray fieldList = data.getJSONArray(KEY_FIELD_LIST);
        if (fieldList == null || fieldList.isEmpty()) {
            return;
        }

        for (int i = 0; i < fieldList.size(); i++) {
            JSONObject field = fieldList.getJSONObject(i);
            KafkaConsumerField consumerField = buildConsumerField(field);
            applyFieldToStepMeta(stepMeta, consumerField, field.getString(FIELD_KAFKA_NAME));
        }
    }

    /**
     * 构建Kafka消费者字段
     */
    private KafkaConsumerField buildConsumerField(JSONObject field) {
        KafkaConsumerField consumerField = new KafkaConsumerField();
        consumerField.setOutputName(field.getString(FIELD_OUTPUT_NAME));

        String outputType = field.getString(FIELD_OUTPUT_TYPE);
        if (StringUtils.isNotBlank(outputType)) {
            consumerField.setOutputType(KafkaConsumerField.Type.valueOf(outputType));
        }

        return consumerField;
    }

    /**
     * 应用字段到步骤元数据
     */
    private void applyFieldToStepMeta(KafkaConsumerInputMeta stepMeta,
                                      KafkaConsumerField field, String kafkaName) {
        if (kafkaName == null) {
            return;
        }

        switch (kafkaName) {
            case Constants.KAFKA_FIELD.KEY:
                field.setKafkaName(KafkaConsumerField.Name.KEY);
                stepMeta.setKeyField(field);
                break;
            case Constants.KAFKA_FIELD.MESSAGE:
                field.setKafkaName(KafkaConsumerField.Name.MESSAGE);
                stepMeta.setMessageField(field);
                break;
            case Constants.KAFKA_FIELD.TOPIC:
                field.setKafkaName(KafkaConsumerField.Name.TOPIC);
                stepMeta.setTopicField(field);
                break;
            case Constants.KAFKA_FIELD.PARTITION:
                field.setKafkaName(KafkaConsumerField.Name.PARTITION);
                stepMeta.setPartitionField(field);
                break;
            case Constants.KAFKA_FIELD.OFFSET:
                field.setKafkaName(KafkaConsumerField.Name.OFFSET);
                stepMeta.setOffsetField(field);
                break;
            case Constants.KAFKA_FIELD.TIMESTAMP:
                field.setKafkaName(KafkaConsumerField.Name.TIMESTAMP);
                stepMeta.setTimestampField(field);
                break;
            default:
                LOGGER.warn("Unknown Kafka field name: {}", kafkaName);
                break;
        }
    }

    /**
     * 配置自定义属性
     */
    private void configureCustomProperties(KafkaConsumerInputMeta stepMeta,
                                           JSONObject data, DbDatabase database) {
        Map<String, String> configMap = new HashMap<>();

        // 加载数据库属性
        loadDatabaseProperties(configMap, database);

        // 加载用户自定义属性
        loadUserProperties(configMap, data);

        stepMeta.setConfig(configMap);
    }

    /**
     * 加载数据库属性
     */
    private void loadDatabaseProperties(Map<String, String> configMap, DbDatabase database) {
        String dbProperties = database.getProperties();
        if (StringUtils.isNotBlank(dbProperties)) {
            try {
                Map<String, String> dbProps = JSONObject.parseObject(dbProperties, HashMap.class);
                if (dbProps != null) {
                    configMap.putAll(dbProps);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to parse database properties: {}", dbProperties, e);
            }
        }
    }

    /**
     * 加载用户自定义属性
     */
    private void loadUserProperties(Map<String, String> configMap, JSONObject data) {
        String properties = data.getString(KEY_PROPERTIES);
        if (StringUtils.isBlank(properties)) {
            return;
        }

        List<PropertiesVo> propertiesVos = JSONArray.parseArray(properties, PropertiesVo.class);
        if (CollectionUtils.isEmpty(propertiesVos)) {
            return;
        }

        for (PropertiesVo prop : propertiesVos) {
            if (StringUtils.isNotBlank(prop.getName())) {
                configMap.put(prop.getName(), prop.getValue());
            }
        }
    }

    /**
     * 构建步骤元数据对象
     */
    private StepMeta buildStepMeta(StepContext context, KafkaConsumerInputMeta stepMeta,
                                   String componentName, JSONObject data) {
        String pluginId = context.getRegistryID().getPluginId(StepPluginType.class, stepMeta);
        if (StringUtils.isBlank(pluginId)) {
            pluginId = KAFKA_CONSUMER_PLUGIN_ID;
            LOGGER.warn("PluginRegistry 未解析到 Kafka 输入步骤 pluginId（步骤：{}），已回退为 {}", componentName, KAFKA_CONSUMER_PLUGIN_ID);
        }

        StepMeta targetStepMeta = context.getStepMetaMap().get(context.getId());
        if (targetStepMeta == null) {
            targetStepMeta = new StepMeta(pluginId, componentName, stepMeta);
        } else {
            targetStepMeta.setStepID(pluginId);
            targetStepMeta.setName(componentName);
            targetStepMeta.setStepMetaInterface(stepMeta);
        }

        return targetStepMeta;
    }

    /**
     * 应用步骤配置
     */
    private void applyStepConfiguration(StepMeta stepMeta, JSONObject data) {
        Optional.ofNullable(data.getInteger(KEY_COPIES_CACHE))
                .ifPresent(stepMeta::setCopies);

        boolean distributeType = data.getBooleanValue(KEY_DISTRIBUTE_TYPE);
        if (distributeType) {
            stepMeta.setDistributes(false);
        }
    }

    /**
     * 构造Kafka地址，多个主机以逗号分隔
     *
     * @param host 主机地址（支持逗号分隔的多个地址）
     * @param port 端口号
     * @return 格式化的Kafka地址
     */
    private String buildKafkaAddress(String host, String port) {
        if (StringUtils.isBlank(host)) {
            return "";
        }

        List<String> hostList = new ArrayList<>();
        String[] hostArray = host.trim().split(",");

        for (String h : hostArray) {
            String trimmedHost = h.trim();
            if (trimmedHost.isEmpty()) {
                continue;
            }

            if (trimmedHost.contains(":") || StringUtils.isBlank(port)) {
                hostList.add(trimmedHost);
            } else {
                hostList.add(trimmedHost + ":" + port);
            }
        }

        return String.join(",", hostList);
    }
}