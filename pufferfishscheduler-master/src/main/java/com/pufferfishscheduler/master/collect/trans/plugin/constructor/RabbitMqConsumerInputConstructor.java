package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.vo.collect.PropertiesVo;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.plugin.RabbitMqConsumerField;
import com.pufferfishscheduler.plugin.RabbitMqConsumerInputMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RabbitMQ 输入：数据源提供 host/port/user/password，组件页提供 vhost、队列、监听与重试等。
 */
@Slf4j
public class RabbitMqConsumerInputConstructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        validateInput(config, context);

        JSONObject root = JSONObject.parseObject(config);
        if (root == null) {
            throw new BusinessException("组件数据不能为空！");
        }
        String name = root.getString("name");
        if (StringUtils.isBlank(name)) {
            throw new BusinessException("组件名称不能为空！");
        }
        JSONObject data = root.getJSONObject("data");
        if (data == null) {
            throw new BusinessException("组件数据不能为空！");
        }

        String dataSourceId = data.getString("dataSourceId");
        if (context.isValidate()) {
            validateBlank(dataSourceId, "【" + name + "】", "数据源");
        }

        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);
        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("数据源不存在!");
        }

        RabbitMqConsumerInputMeta meta = new RabbitMqConsumerInputMeta();
        meta.setDefault();
        meta.setHost(database.getDbHost());
        meta.setPort(StringUtils.defaultIfBlank(database.getDbPort(), Constants.RABBITMQ_COMMON_CONFIG.DEFAULT_PORT));
        meta.setUsername(StringUtils.defaultString(database.getUsername(), Constants.RABBITMQ_COMMON_CONFIG.DEFAULT_USERNAME));
        meta.setPassword(StringUtils.defaultString(database.getPassword(), Constants.RABBITMQ_COMMON_CONFIG.DEFAULT_PASSWORD));

        meta.setVirtualHost(StringUtils.defaultIfBlank(data.getString("virtualHost"), "/"));

        String queue = data.getString("queue");
        if (StringUtils.isBlank(queue) && data.getJSONArray("topics") != null && !data.getJSONArray("topics").isEmpty()) {
            queue = data.getJSONArray("topics").getString(0);
        }
        meta.setQueue(StringUtils.defaultString(queue, ""));

        String ack = data.getString("listenerAcknowledgeMode");
        if (StringUtils.isBlank(ack)
                || "manual".equalsIgnoreCase(ack)
                || Constants.COMMIT_TYPE.MANUAL_COMMIT.equals(ack)) {
            meta.setAutoAck(false);
        } else {
            meta.setAutoAck(true);
        }

        meta.setPrefetch(data.getIntValue("listenerPrefetch", 1));
        meta.setConcurrency(Math.max(1, data.getIntValue("listenerConcurrency", 1)));
        meta.setMaxConcurrency(Math.max(1, data.getIntValue("listenerMaxConcurrency", 1)));
        if (data.containsKey("listenerDefaultRequeueRejected")) {
            meta.setDefaultRequeueRejected(data.getBooleanValue("listenerDefaultRequeueRejected"));
        } else {
            meta.setDefaultRequeueRejected(true);
        }
        if (data.containsKey("listenerRetryEnabled")) {
            meta.setRetryEnabled(data.getBooleanValue("listenerRetryEnabled"));
        } else {
            meta.setRetryEnabled(true);
        }
        meta.setRetryInitialIntervalMs(data.getLongValue("listenerRetryInitialInterval", 1000L));
        meta.setRetryMaxAttempts(Math.max(1, data.getIntValue("listenerRetryMaxAttempts", 3)));
        meta.setRetryMaxIntervalMs(data.getLongValue("listenerRetryMaxInterval", 10000L));
        Double mul = data.getDouble("listenerRetryMultiplier");
        meta.setRetryMultiplier(mul == null ? 1.0D : mul);
        meta.setMaxMessages(data.getLongValue("maxMessages", 0L));

        JSONArray fieldList = data.getJSONArray("fieldList");
        if (fieldList != null && !fieldList.isEmpty()) {
            for (int i = 0; i < fieldList.size(); i++) {
                JSONObject field = fieldList.getJSONObject(i);
                String rabbitName = field.getString("rabbitName");
                RabbitMqConsumerField f = new RabbitMqConsumerField();
                f.setOutputName(field.getString("outputName"));
                String ot = field.getString("outputType");
                f.setOutputType(
                        StringUtils.isBlank(ot)
                                ? RabbitMqConsumerField.Type.String
                                : RabbitMqConsumerField.Type.valueOf(ot));
                switch (StringUtils.defaultString(rabbitName, "")) {
                    case Constants.RABBITMQ_FIELD.MESSAGE -> meta.setMessageField(applyName(f, RabbitMqConsumerField.Name.MESSAGE));
                    case Constants.RABBITMQ_FIELD.ROUTING_KEY -> meta.setRoutingKeyField(applyName(f, RabbitMqConsumerField.Name.ROUTING_KEY));
                    case Constants.RABBITMQ_FIELD.MESSAGE_ID -> meta.setMessageIdField(applyName(f, RabbitMqConsumerField.Name.MESSAGE_ID));
                    case Constants.RABBITMQ_FIELD.DELIVERY_TAG -> meta.setDeliveryTagField(applyName(f, RabbitMqConsumerField.Name.DELIVERY_TAG));
                    case Constants.RABBITMQ_FIELD.EXCHANGE -> meta.setExchangeField(applyName(f, RabbitMqConsumerField.Name.EXCHANGE));
                    case Constants.RABBITMQ_FIELD.TIMESTAMP -> meta.setTimestampField(applyName(f, RabbitMqConsumerField.Name.TIMESTAMP));
                    default -> {
                    }
                }
            }
        }

        Map<String, String> cfg = new HashMap<>();
        if (StringUtils.isNotBlank(database.getProperties())) {
            try {
                Map<String, String> dbProps = JSONObject.parseObject(database.getProperties(), HashMap.class);
                if (dbProps != null) {
                    cfg.putAll(dbProps);
                }
            } catch (Exception e) {
                log.warn("解析数据源 properties 失败: {}", database.getProperties(), e);
            }
        }
        String properties = data.getString("properties");
        if (StringUtils.isNotBlank(properties)) {
            List<PropertiesVo> vos = JSONArray.parseArray(properties, PropertiesVo.class);
            if (CollectionUtils.isNotEmpty(vos)) {
                for (PropertiesVo p : vos) {
                    if (StringUtils.isNotBlank(p.getName())) {
                        cfg.put(p.getName(), p.getValue() == null ? "" : p.getValue());
                    }
                }
            }
        }
        meta.setConfig(cfg);

        String pluginId = context.getRegistryID().getPluginId(StepPluginType.class, meta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (stepMeta == null) {
            stepMeta = new StepMeta(pluginId, name, meta);
        } else {
            stepMeta.setStepID(pluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(meta);
        }
        if (data.getInteger("copiesCache") != null) {
            stepMeta.setCopies(data.getInteger("copiesCache"));
        }
        if (data.getBooleanValue("distributeType")) {
            stepMeta.setDistributes(false);
        }
        stepMeta.setDraw(true);
        return stepMeta;
    }

    /**
     * 应用字段名。
     */
    private static RabbitMqConsumerField applyName(RabbitMqConsumerField f, RabbitMqConsumerField.Name name) {
        f.setRabbitName(name);
        return f;
    }
}
