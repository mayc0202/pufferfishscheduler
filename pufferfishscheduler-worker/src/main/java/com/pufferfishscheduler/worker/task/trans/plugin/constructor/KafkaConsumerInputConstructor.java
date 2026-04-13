package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.TransFlow;
import com.pufferfishscheduler.dao.mapper.TransFlowMapper;
import com.pufferfishscheduler.domain.vo.collect.PropertiesVo;
import com.pufferfishscheduler.plugin.KafkaConsumerField;
import com.pufferfishscheduler.plugin.KafkaConsumerInputMeta;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import com.pufferfishscheduler.worker.task.trans.engine.DataFlowRepository;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransFlowConfig;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
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

/**
 * Kafka输入组件构造器
 */
public class KafkaConsumerInputConstructor extends AbstractStepMetaConstructor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerInputConstructor.class);

    private static final String KAFKA_CONSUMER_PLUGIN_ID = "KafkaConsumerInput";

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {

        // 验证输入参数
        validateInput(config, context);

        // 从应用上下文获取数据库服务
        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);

        JSONObject jsonObject = JSONObject.parseObject(config);
        if (jsonObject == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        // 从配置中提取组件属性
        String name = jsonObject.getString("name"); // 组件名称
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }

        // 从配置中提取组件数据
        JSONObject data = jsonObject.getJSONObject("data");
        if (data == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        String dataSourceId = data.getString("dataSource");

        if (context.isValidate()) {
            validateBlank(dataSourceId, "【" + name + "】", "数据源");
        }

        // 构造示例
        KafkaConsumerInputMeta kafkaConsumerInputMeta = new KafkaConsumerInputMeta();
        kafkaConsumerInputMeta.setDefault();

        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("数据源不存在!");
        }

        //连接方式
        kafkaConsumerInputMeta.setConnectionType(KafkaConsumerInputMeta.ConnectionType.DIRECT);

        //kafka地址
        kafkaConsumerInputMeta.setDirectBootstrapServers(buildKafkaAddress(database.getDbHost(), database.getDbPort()));

        JSONArray jsonArray = data.getJSONArray("topics");

        if (null == jsonArray) {
            jsonArray = new JSONArray();
        }

        // 主题
        List<String> topics = jsonArray.toJavaList(String.class);
        kafkaConsumerInputMeta.setTopics(topics);

        // 消费组
        String consumerGroup = data.getString("consumerGroup");
        kafkaConsumerInputMeta.setConsumerGroup(consumerGroup);

        // 每次获取消息持续时长
        String batchDuration = data.getString("batchDuration");
        kafkaConsumerInputMeta.setBatchDuration(batchDuration);

        // 每次最大获取消息数量
        String batchSize = data.getString("batchSize");
        kafkaConsumerInputMeta.setBatchSize(batchSize);

        // 最大预取记录数
        String prefetchCount = data.getString("prefetchCount");
        if (StringUtils.isNotBlank(prefetchCount)) {
            kafkaConsumerInputMeta.setPrefetchCount(prefetchCount);
        }

        // 最大预取记录数
        String partition = data.getString("partition");
        if (StringUtils.isNotBlank(partition)) {
            kafkaConsumerInputMeta.setPartition(partition);
        }

        // 偏移量提交方式
        String commitType = data.getString("commitType");
        kafkaConsumerInputMeta.setAutoCommit(Constants.COMMIT_TYPE.AUTO_COMMIT.equals(commitType));

        // 输出字段
        JSONArray fieldList = data.getJSONArray("fieldList");
        if (null != fieldList && !fieldList.isEmpty()) {
            for (int i = 0; i < fieldList.size(); i++) {
                JSONObject field = fieldList.getJSONObject(i);
                String kafkaName = field.getString("kafkaName");
                KafkaConsumerField kafkaConsumerField = new KafkaConsumerField();
                kafkaConsumerField.setOutputName(field.getString("outputName"));
                kafkaConsumerField.setOutputType(KafkaConsumerField.Type.valueOf(field.getString("outputType")));
                switch (kafkaName) {
                    case Constants.KAFKA_FIELD.KEY:
                        kafkaConsumerField.setKafkaName(KafkaConsumerField.Name.KEY);
                        kafkaConsumerInputMeta.setKeyField(kafkaConsumerField);
                        break;
                    case Constants.KAFKA_FIELD.MESSAGE:
                        kafkaConsumerField.setKafkaName(KafkaConsumerField.Name.MESSAGE);
                        kafkaConsumerInputMeta.setMessageField(kafkaConsumerField);
                        break;
                    case Constants.KAFKA_FIELD.TOPIC:
                        kafkaConsumerField.setKafkaName(KafkaConsumerField.Name.TOPIC);
                        kafkaConsumerInputMeta.setTopicField(kafkaConsumerField);
                        break;
                    case Constants.KAFKA_FIELD.PARTITION:
                        kafkaConsumerField.setKafkaName(KafkaConsumerField.Name.PARTITION);
                        kafkaConsumerInputMeta.setPartitionField(kafkaConsumerField);
                        break;
                    case Constants.KAFKA_FIELD.OFFSET:
                        kafkaConsumerField.setKafkaName(KafkaConsumerField.Name.OFFSET);
                        kafkaConsumerInputMeta.setOffsetField(kafkaConsumerField);
                        break;
                    case Constants.KAFKA_FIELD.TIMESTAMP:
                        kafkaConsumerField.setKafkaName(KafkaConsumerField.Name.TIMESTAMP);
                        kafkaConsumerInputMeta.setTimestampField(kafkaConsumerField);
                        break;
                    default:
                        break;
                }
            }
        }

        // 用户自定义配置
        String properties = data.getString("properties");
        List<PropertiesVo> propertiesVos = JSONArray.parseArray(properties, PropertiesVo.class);
        Map<String, String> map = new HashMap<>();
        HashMap hashMap = new HashMap<String, String>();
        if (StringUtils.isNotBlank(database.getProperties())) {
            hashMap = JSONObject.parseObject(database.getProperties(), HashMap.class);
        }
        if (null == hashMap) {
            hashMap = new HashMap<>();
        }
        if (CollectionUtils.isNotEmpty(propertiesVos)) {
            for (PropertiesVo propertiesVo : propertiesVos) {
                map.put(propertiesVo.getName(), propertiesVo.getValue());
            }
            hashMap.forEach((k, v) -> map.merge((String) k, (String) v, (v1, v2) -> v2));
        }
        kafkaConsumerInputMeta.setConfig(map);

        String subPath = data.getString("subTransformationPath");
        if (StringUtils.isNotBlank(subPath)) {
            kafkaConsumerInputMeta.setTransformationPath(subPath);
            kafkaConsumerInputMeta.setFileName(subPath);
        }
        String subStepName = data.getString("subStep");
        if (StringUtils.isNotBlank(subStepName)) {
            kafkaConsumerInputMeta.setSubStep(subStepName);
        }

        String transId = data.getString("transId");
        if (StringUtils.isNotBlank(transId)) {
            String transIdTrim = transId.trim();
            try {
                TransFlowMapper transFlowMapper = PufferfishSchedulerApplicationContext.getBean(TransFlowMapper.class);
                TransFlow tf = transFlowMapper.selectById(Integer.parseInt(transIdTrim));
                if (tf != null && StringUtils.isNotBlank(tf.getStage())) {
                    String biz = tf.getStage() + "_" + Constants.TRANS;
                    kafkaConsumerInputMeta.setBizType(biz);
                    kafkaConsumerInputMeta.setBizObjectId(transIdTrim);
                    if (StringUtils.isBlank(kafkaConsumerInputMeta.getTransformationPath())) {
                        String ktrPath = materializeSubTransKtrFromRepository(biz, transIdTrim);
                        if (StringUtils.isNotBlank(ktrPath)) {
                            kafkaConsumerInputMeta.setTransformationPath(ktrPath);
                            kafkaConsumerInputMeta.setFileName(ktrPath);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("解析子转换 transId={} 失败: {}", transIdTrim, e.getMessage());
            }
        }

        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, kafkaConsumerInputMeta);
        if (StringUtils.isBlank(eiPluginId)) {
            eiPluginId = KAFKA_CONSUMER_PLUGIN_ID;
            LOGGER.warn("PluginRegistry 未解析到 Kafka 输入步骤 pluginId（步骤：{}），已回退为 {}", name, KAFKA_CONSUMER_PLUGIN_ID);
        }
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, kafkaConsumerInputMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(kafkaConsumerInputMeta);
        }

        Integer copiesCache = data.getInteger("copiesCache");
        if (null != copiesCache) {
            stepMeta.setCopies(copiesCache);
        }
        //判断是否为复制流程
        boolean distributeType = data.getBooleanValue("distributeType");
        if (distributeType) {
            stepMeta.setDistributes(false);
        }

        return stepMeta;
    }

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
     * 构造kafka地址，多个主机以逗号分隔，如果主机中没有包含端口，加上端口。
     *
     * @param host
     * @param port
     * @return
     */
    private String buildKafkaAddress(String host, String port) {
        if (StringUtils.isBlank(host)) {
            return "";
        }
        List<String> hostList = new ArrayList<>();
        String[] hostArray = host.trim().split(",");
        for (int i = 0; i < hostArray.length; i++) {
            String h = hostArray[i];
            if (h.contains(":") || StringUtils.isBlank(port)) {
                hostList.add(h);
            } else {
                hostList.add(h + ":" + port);
            }
        }
        return StringUtils.join(hostList, ",");
    }
}
