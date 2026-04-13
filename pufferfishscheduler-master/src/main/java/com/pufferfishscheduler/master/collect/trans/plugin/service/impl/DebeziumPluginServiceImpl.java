package com.pufferfishscheduler.master.collect.trans.plugin.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.plugin.service.DebeziumPluginService;
import com.pufferfishscheduler.plugin.common.domain.DebeziumField;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;

/**
 * Debezium格式JSON转换插件实现类
 */
@Service
public class DebeziumPluginServiceImpl implements DebeziumPluginService {

    /**
     * 结构报文数据
     *
     * @param sample
     */
    @Override
    public List<DebeziumField>  parseSampleData(JSONObject sample) {
        String jsonTxt = sample.getString("sample");
        // 拉平输出字段(有序)
        List<DebeziumField> outputFields = new LinkedList<>();
        JSONObject json = JSONObject.parseObject(jsonTxt);
        if (null == json) {
            throw new BusinessException("请校验json报文是否为空！");
        }

        JSONObject payload = json.getJSONObject(Constants.DEBEZIUM_JSON.PAY_LOAD);
        if (payload == null) {
            throw new BusinessException("请校验json消息payload载体是否为空！");
        }

        String op = payload.getString(Constants.DEBEZIUM_JSON.OP);
        if (StringUtils.isBlank(op)) {
            throw new BusinessException("请校验数据格式是否正确！");
        }

        // 如果Kafka开启了value.converter.schemas.enable，那么可以获取到schema信息
        JSONObject schema = json.getJSONObject(Constants.DEBEZIUM_JSON.SCHEMA);
        if (null == schema) {
            throw new BusinessException("请校验数据格式是否正确！");
        }

        if (!schema.getJSONArray(Constants.DEBEZIUM_JSON.FIELDS).isEmpty()) {
            JSONArray fields = schema.getJSONArray(Constants.DEBEZIUM_JSON.FIELDS);
            for (Object fieldObj : fields) {
                JSONObject field = JSONObject.parseObject(fieldObj.toString());
                String fieldType = field.getString(Constants.DEBEZIUM_JSON.FIELD);
                if (Constants.DEBEZIUM_JSON.BEFORE.equals(fieldType)) {
                    JSONArray fieldArray = field.getJSONArray(Constants.DEBEZIUM_JSON.FIELDS);
                    addFieldType(fieldArray, outputFields);
                } else if (Constants.DEBEZIUM_JSON.OP.equals(fieldType)) {
                    DebeziumField opField = JSONObject.parseObject(field.toJSONString(), DebeziumField.class);
                    opField.setField(Constants.DEBEZIUM_JSON.OP);
                    // 设置平台统一数据类型
                    opField.setDataType(setFieldType(opField.getType(),opField.getName()));
                    outputFields.add(opField);
                }
            }
        }

        return outputFields;
    }

    /**
     * 设置字段类型
     *
     * @param fieldArray
     */
    public void addFieldType(JSONArray fieldArray, List<DebeziumField> list) {
        if (!fieldArray.isEmpty()) {
            for (Object o : fieldArray) {
                DebeziumField field = JSONObject.parseObject(o.toString(), DebeziumField.class);
                // 设置平台统一数据类型
                field.setDataType(setFieldType(field.getType(),field.getName()));
                list.add(field);
            }
        }
    }


    /**
     * 转成平台字段类型
     * @param type
     * @param name
     * @return
     */
    public String setFieldType(String type, String name) {
        switch (type) {
            case "float":
            case "double":
                return "Number";
            case "boolean":
                return "Boolean";
            case "int16":
            case "int32":
            case "long":
            case "int":
                return "Integer";
            case "bignumber":
                return "Bignumber";
            case "binary":
            case "bytes":
                return "Binary";
            case "int64":
                if (StringUtils.isNotBlank(name) && ("org.apache.kafka.connect.data.Timestamp".equals(name) ||
                        "io.debezium.time.ZonedTimestamp".equals(name) || "io.debezium.time.Timestamp".equals(name))) {
                    return "Timestamp";
                } else {
                    return "Integer";
                }
            case "timestamp":
                return "Timestamp";
            case "null":
            case "record":
            case "enum":
            case "array":
            case "map":
            case "union":
            case "fixed":
            case "string":
            default:
                return "String";
        }
    }
}
