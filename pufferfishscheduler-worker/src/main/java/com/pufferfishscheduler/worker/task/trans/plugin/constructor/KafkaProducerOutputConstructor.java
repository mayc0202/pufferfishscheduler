package com.pufferfishscheduler.worker.task.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.vo.collect.PropertiesVo;
import com.pufferfishscheduler.plugin.KafkaProducerOutputMeta;
import com.pufferfishscheduler.worker.task.metadata.service.DbDatabaseService;
import com.pufferfishscheduler.worker.task.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.worker.task.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 输出：将前端 JSON 转为 {@link KafkaProducerOutputMeta}。
 */
public class KafkaProducerOutputConstructor extends AbstractStepMetaConstructor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerOutputConstructor.class);

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

        String dataSourceId = data.getString("dataSource");
        if (context.isValidate()) {
            validateBlank(dataSourceId, "【" + name + "】", "数据源");
        }

        DbDatabaseService databaseService = PufferfishSchedulerApplicationContext.getBean(DbDatabaseService.class);
        DbDatabase database = databaseService.getDatabaseById(Integer.valueOf(dataSourceId));
        if (database == null) {
            throw new BusinessException("数据源不存在!");
        }

        KafkaProducerOutputMeta meta = new KafkaProducerOutputMeta();
        meta.setDefault();
        meta.setConnectionType(KafkaProducerOutputMeta.ConnectionType.DIRECT);
        meta.setDirectBootstrapServers(buildKafkaAddress(database.getDbHost(), database.getDbPort()));

        String topic = data.getString("topic");
        if (StringUtils.isNotBlank(topic)) {
            meta.setTopic(topic);
        }
        applyIfNotBlank(data.getString("clientId"), meta::setClientId);
        applyIfNotBlank(data.getString("keyField"), meta::setKeyField);
        applyIfNotBlank(data.getString("messageField"), meta::setMessageField);

        Map<String, String> cfg = new HashMap<>();
        if (StringUtils.isNotBlank(database.getProperties())) {
            try {
                Map<String, String> dbProps = JSONObject.parseObject(database.getProperties(), HashMap.class);
                if (dbProps != null) {
                    cfg.putAll(dbProps);
                }
            } catch (Exception e) {
                LOGGER.warn("解析数据源 properties 失败: {}", database.getProperties(), e);
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

    private static void applyIfNotBlank(String v, java.util.function.Consumer<String> setter) {
        if (StringUtils.isNotBlank(v)) {
            setter.accept(v);
        }
    }

    private String buildKafkaAddress(String host, String port) {
        if (StringUtils.isBlank(host)) {
            return "";
        }
        List<String> hostList = new ArrayList<>();
        for (String h : host.trim().split(",")) {
            String trimmed = h.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.contains(":") || StringUtils.isBlank(port)) {
                hostList.add(trimmed);
            } else {
                hostList.add(trimmed + ":" + port);
            }
        }
        return String.join(",", hostList);
    }
}
