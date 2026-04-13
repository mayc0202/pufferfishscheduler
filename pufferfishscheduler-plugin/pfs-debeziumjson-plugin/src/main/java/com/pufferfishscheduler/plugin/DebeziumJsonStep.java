package com.pufferfishscheduler.plugin;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONPath;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.domain.DebeziumField;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

public class DebeziumJsonStep extends BaseStep implements StepInterface {

    private DebeziumJsonStepMeta meta;
    private DebeziumJsonStepData data;

    // 输出字段配置映射
    private final Map<String, DebeziumField> outputFieldConfigMap = new LinkedHashMap<>();

    // 行结构相关
    private int inputRowLength;
    private int sourceFieldIndex;

    public DebeziumJsonStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                            int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * 初始化：加载字段配置
     */
    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }

        this.data = (DebeziumJsonStepData) sdi;
        this.meta = (DebeziumJsonStepMeta) smi;

        try {
            initOutputFieldConfig();
            logDebug("DebeziumJsonStep 初始化完成");
            return true;
        } catch (KettleException e) {
            logError("DebeziumJsonStep 初始化失败", e);
            return false;
        }
    }

    /**
     * 初始化输出字段配置
     */
    private void initOutputFieldConfig() throws KettleException {
        String[] fieldConfigs = meta.getOutputFieldConfig();
        if (fieldConfigs == null || fieldConfigs.length == 0) {
            throw new KettleStepException("未配置任何输出字段");
        }

        outputFieldConfigMap.clear();
        for (String json : fieldConfigs) {
            DebeziumField field = JSONObject.parseObject(json, DebeziumField.class);
            if (field == null) {
                throw new KettleStepException("解析字段配置失败：" + json);
            }
            // 兼容旧配置：自动推断类型
            if (StringUtils.isBlank(field.getType())) {
                inferFieldType(field);
            }
            outputFieldConfigMap.put(field.getField(), field);
        }
    }

    /**
     * 从 dataType 自动推断 type
     */
    private void inferFieldType(DebeziumField field) {
        String dataType = field.getDataType();
        if (dataType == null) {
            field.setType("string");
            return;
        }

        switch (dataType) {
            case "Number":
            case "Bignumber":
                field.setType("double");
                break;
            case "Boolean":
                field.setType("boolean");
                break;
            case "Integer":
                field.setType("int32");
                break;
            case "Binary":
            case "bytes":
                field.setType("bytes");
                break;
            case "Timestamp":
            case "Date":
                field.setType("int64");
                field.setName(Constants.TIMESTAMP_SCHEMA_NAME);
                break;
            default:
                field.setType("string");
        }
    }

    /**
     * 处理每一行数据
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        Object[] row = getRow();
        if (row == null) {
            setOutputDone();
            return false;
        }

        // 首次执行：初始化行结构
        if (first) {
            first = false;
            data.outputRowMeta = getInputRowMeta().clone();
            inputRowLength = getInputRowMeta().size();
            sourceFieldIndex = getInputRowMeta().indexOfValue(meta.getSourceField());

            if (sourceFieldIndex < 0) {
                throw new KettleStepException("输入行中未找到源字段：" + meta.getSourceField());
            }

            // 注册输出字段
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        }

        // 提取 Debezium 字段
        Object jsonValue = row[sourceFieldIndex];
        Object[] extracted = extractAndConvertFields(jsonValue);

        // 构建输出行
        Object[] outputRow = buildOutputRow(row, extracted);
        putRow(data.outputRowMeta, outputRow);

        return true;
    }

    /**
     * 从 Debezium JSON 提取并转换所有配置的字段
     */
    private Object[] extractAndConvertFields(Object source) throws KettleException {
        if (source == null) {
            return new Object[outputFieldConfigMap.size()];
        }

        String json = source.toString();
        String op = JSONPath.extract(json, "$.payload.op").toString();

        if (StringUtils.isBlank(op)) {
            throw new KettleStepException("Debezium 数据缺少 op 字段");
        }

        // 确定读取 before 还是 after
        String dataPath = getPayloadPath(op);
        Object dataObj = JSONPath.eval(json, dataPath);

        if (!(dataObj instanceof Map)) {
            throw new KettleStepException("解析失败，路径不是 JSON 对象：" + dataPath);
        }

        Map<String, Object> dataMap = (Map<String, Object>) dataObj;
        dataMap.put(Constants.OP, op);

        // 转换所有字段
        Object[] result = new Object[outputFieldConfigMap.size()];
        int idx = 0;
        for (DebeziumField field : outputFieldConfigMap.values()) {
            Object raw = dataMap.get(field.getField());
            result[idx++] = convertValue(raw, field);
        }
        return result;
    }

    /**
     * 根据操作类型获取 payload 路径
     */
    private String getPayloadPath(String op) throws KettleStepException {
        return switch (op) {
            case Constants.OP_READ, Constants.OP_CREATE, Constants.OP_UPDATE -> "$.payload.after";
            case Constants.OP_DELETE -> "$.payload.before";
            default -> throw new KettleStepException("不支持的操作类型：" + op);
        };
    }

    /**
     * 【核心】根据字段定义转换值（支持长度、精度）
     */
    private Object convertValue(Object value, DebeziumField field) {
        if (value == null || field == null) {
            return null;
        }

        String type = field.getType();
        try {
            return switch (type) {
                case "float", "double" -> convertToDouble(value, field);
                case "boolean" -> Boolean.valueOf(value.toString());
                case "int", "int16", "int32", "long" -> Long.valueOf(value.toString());
                case "int64" -> convertToInt64(value, field);
                case "binary", "bytes" -> value.toString().getBytes();
                case "string" -> value.toString();
                default -> value.toString();
            };
        } catch (Exception e) {
            logError("字段转换失败：{}，类型：{}，原始值：{}", field.getField(), type, value, e);
            return null;
        }
    }

    /**
     * 转换浮点型：支持精度、长度
     */
    private Object convertToDouble(Object value, DebeziumField field) {
        BigDecimal bd = new BigDecimal(value.toString());

        // 精度（小数位）
        String precision = field.getPrecision();
        if (StringUtils.isNotBlank(precision)) {
            int scale = Integer.parseInt(precision.trim());
            bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
        }

        return bd.doubleValue();
    }

    /**
     * 转换 int64：时间戳特殊处理
     */
    private Object convertToInt64(Object value, DebeziumField field) {
        long ts = Long.parseLong(value.toString());
        String schema = field.getName();

        if (Constants.TIMESTAMP_SCHEMA_NAME.equals(schema)
                || Constants.ZONED_TIMESTAMP_SCHEMA_NAME.equals(schema)
                || Constants.APACHE_TIMESTAMP_SCHEMA_NAME.equals(schema)) {
            return new Timestamp(ts);
        }
        return ts;
    }

    /**
     * 构建最终输出行
     */
    private Object[] buildOutputRow(Object[] input, Object[] extracted) {
        int newSize = inputRowLength + outputFieldConfigMap.size();
        Object[] output = Arrays.copyOf(input, newSize);

        if (extracted != null && extracted.length == outputFieldConfigMap.size()) {
            System.arraycopy(extracted, 0, output, inputRowLength, extracted.length);
        } else {
            Arrays.fill(output, inputRowLength, newSize, null);
        }
        return output;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
        logDebug("DebeziumJsonStep 已释放资源");
    }
}