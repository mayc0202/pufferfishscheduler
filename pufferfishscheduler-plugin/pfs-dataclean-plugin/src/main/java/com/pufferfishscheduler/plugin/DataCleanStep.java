package com.pufferfishscheduler.plugin;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.util.HttpClientUtils;
import com.pufferfishscheduler.plugin.processor.RuleProcessor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

public class DataCleanStep extends BaseStep {
    private DataCleanStepMeta meta;
    private DataCleanStepData data;

    private static final class ProcessorDescriptor {
        private final Integer processorId;
        private final String processorClass;
        private final String config;

        private ProcessorDescriptor(Integer processorId, String processorClass, String config) {
            this.processorId = processorId;
            this.processorClass = processorClass;
            this.config = config;
        }
    }

    /**
     * 规则绑定
     */
    private static final class RuleBinding {
        private final int bindingIndex;
        private final int inputFieldIndex;
        private final RuleProcessor processor;

        private RuleBinding(int bindingIndex, int inputFieldIndex, RuleProcessor processor) {
            this.bindingIndex = bindingIndex;
            this.inputFieldIndex = inputFieldIndex;
            this.processor = processor;
        }
    }

    /**
     * 编译后的规则绑定（按配置行顺序）
     */
    private List<RuleBinding> bindings = Collections.emptyList();

    // 输入字段数（初始化后固定）
    private int inputRowLength = 0;
    // 新增字段数（初始化后固定）
    private int newFieldLength = 0;

    /**
     * 构造函数
     */
    public DataCleanStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * 初始化操作
     */
    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        // 初始化操作
        if (super.init(smi, sdi)) {
            // 获取流中参数：字段名、规则处理器全路径
            this.data = (DataCleanStepData) sdi;
            this.meta = (DataCleanStepMeta) smi;
            return true;
        }
        logDebug("DataClean Step Init ...");
        return false;
    }

    /**
     * init数据
     */
    private void initData() throws KettleStepException {
        String[] ruleIds = this.meta.getRuleId();
        String[] fieldNames = this.meta.getFieldName();
        String[] params = this.meta.getParams();

        if (ruleIds == null || fieldNames == null || params == null) {
            throw new KettleStepException("请校验数据清洗组件配置：ruleId/fieldName/params 不能为空");
        }
        if (ruleIds.length != fieldNames.length || ruleIds.length != params.length || ruleIds.length != this.meta.getRename().length) {
            throw new KettleStepException("请校验数据清洗组件配置：fieldName/params/rename/ruleId 长度必须一致");
        }

        // 1) 拉取规则处理器描述（按ruleId批量）
        Map<String, ProcessorDescriptor> descriptorMap = loadProcessorDescriptors(
                this.meta.getAddress(),
                Arrays.asList(ruleIds)
        );

        // 2) 编译为bindings：一次性定位字段下标、一次性初始化处理器
        List<RuleBinding> out = new ArrayList<>(ruleIds.length);
        for (int i = 0; i < ruleIds.length; i++) {
            String ruleId = ruleIds[i];
            ProcessorDescriptor desc = descriptorMap.get(ruleId);
            if (desc == null) {
                throw new KettleStepException("未获取到规则实例：" + ruleId + "，请校验规则是否被禁用或删除！");
            }

            int inputIdx = this.data.outputRowMeta.indexOfValue(fieldNames[i]);
            if (inputIdx < 0) {
                throw new KettleStepException(fieldNames[i] + " 字段不存在");
            }

            RuleProcessor instance = newProcessorInstance(desc.processorClass);
            // Java自定义与值映射 → 使用已发布配置；其它规则 → 使用步骤参数配置
            if (Objects.equals(desc.processorId, Constants.PROCESSOR_ID.JAVA_CUSTOM) || Objects.equals(desc.processorId, Constants.PROCESSOR_ID.VALUE_MAPPING)) {
                if (desc.config == null || desc.config.isBlank()) {
                    throw new KettleStepException("规则 " + ruleId + " 已发布配置为空，请检查规则发布状态");
                }
                instance.init(JSONObject.parseObject(desc.config));
            } else {
                String config = params[i];
                if (config == null || config.isBlank()) {
                    throw new KettleStepException("规则 " + ruleId + " 参数配置为空，请检查组件配置");
                }
                instance.init(JSONObject.parseObject(config));
            }

            out.add(new RuleBinding(
                    i,
                    inputIdx,
                    instance
            ));
        }

        this.bindings = Collections.unmodifiableList(out);
    }

    /**
     * 批量拉取处理器描述（ruleId -> descriptor）
     */
    private Map<String, ProcessorDescriptor> loadProcessorDescriptors(String address, List<String> ruleIdList) throws KettleStepException {
        try {
            String ruleConfig = getRuleConfig(address, ruleIdList);
            JSONArray array = JSONArray.parseArray(ruleConfig);
            Map<String, ProcessorDescriptor> map = new HashMap<>(Math.max(16, array.size() * 2));
            for (int i = 0; i < array.size(); i++) {
                JSONObject obj = JSONObject.parseObject(array.get(i).toString());
                // 规则Id
                String ruleId = obj.getString("id");
                // 处理器id
                Integer processorId = obj.getInteger("processorId");
                String processorClass = obj.getString("processorClass");
                String config = obj.getString("config");
                map.put(ruleId, new ProcessorDescriptor(processorId, processorClass, config));
            }
            return map;
        } catch (Exception e) {
            logError(String.format("处理规则实例异常：" + e.getMessage()));
            throw new KettleStepException("处理规则实例异常：" + e.getMessage());
        }
    }

    private RuleProcessor newProcessorInstance(String processorClass) throws KettleStepException {
        if (processorClass == null || processorClass.isBlank()) {
            throw new KettleStepException("processorClass 不能为空");
        }
        try {
            Class<?> clazz = Class.forName(processorClass);
            Object o = clazz.getDeclaredConstructor().newInstance();
            if (!(o instanceof RuleProcessor)) {
                throw new KettleStepException("处理器未实现 RuleProcessor：" + processorClass);
            }
            return (RuleProcessor) o;
        } catch (Exception e) {
            throw new KettleStepException("实例化处理器失败：" + processorClass + "，" + e.getMessage());
        }
    }

    /**
     * 获取已发布配置
     *
     * @param address 规则服务地址
     * @param ruleIdList 规则ID列表
     * @return 规则信息列表
     */
    private String getRuleConfig(String address, List<String> ruleIdList) throws KettleException {
        String config = "";
        JSONArray requestBody = new JSONArray();
        requestBody.addAll(ruleIdList);
        Map<String, String> headers = new HashMap<>();
        String result = HttpClientUtils.postJson(address, headers, requestBody.toString());
        if (result == null || result.isBlank()) {
            throw new KettleStepException("获取规则配置失败：规则服务返回为空，请检查地址、鉴权及服务可用性。address=" + address);
        }

        JSONObject resultObj = JSONObject.parseObject(result);
        if (resultObj == null) {
            throw new KettleStepException("获取规则配置失败：规则服务返回非JSON内容。address=" + address + ", result=" + result);
        }
        String status = resultObj.getString("status");
        if (status == null || status.isBlank()) {
            throw new KettleStepException("获取规则配置失败：响应缺少 status 字段。address=" + address + ", result=" + result);
        }
        if ("ERROR".equals(status)) {
            throw new KettleStepException(resultObj.getString("message"));
        }
        JSONArray response = resultObj.getJSONArray("data");
        if (response != null) {
            config = response.toString();
        } else {
            throw new KettleStepException("获取规则配置失败：响应缺少 data 字段。address=" + address + ", result=" + result);
        }
        return config;
    }

    /**
     * 流中数据操作
     *
     * @param smi
     * @param sdi
     * @return
     * @throws KettleException
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.data = (DataCleanStepData) sdi;
        this.meta = (DataCleanStepMeta) smi;
        Object[] r = getRow();
        if (r == null) {
            this.setOutputDone();
            return false;
        }
        //将数据输出
        try {
            if (first) {
                first = false;
                //复制输出数据元数据信息
                this.data.outputRowMeta = getInputRowMeta().clone();
                // 获取准确的流中字段长度（初始化后固定）
                this.inputRowLength = data.outputRowMeta.getValueMetaList().size();
                //如果该转换存在自定义其他输出字段，那么需要调用此方法生成新的字段
                this.meta.getFields(data.outputRowMeta, this.getStepname(), null, null, this, repository, metaStore);
                // 记录新增字段长度
                this.newFieldLength = this.meta.getRename().length;

                // 依赖 outputRowMeta 定位输入字段下标，因此必须在 clone 后再 init
                this.initData();
            }

            Object[] objects = this.setRowValue(r, this.dealRowData(r));
            this.putRow(this.data.outputRowMeta, objects);
        } catch (Exception e) {
            logError(e.getMessage());
            setErrors(1);
            stopAll();
            setOutputDone();
            return false;
        }
        return true;
    }

    /**
     * 处理流中数据
     *
     * @param r
     * @throws KettleException
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    private Object[] dealRowData(Object[] r) throws KettleException, SQLException, UnsupportedEncodingException {
        Object[] cache = new Object[this.newFieldLength];
        for (RuleBinding b : this.bindings) {
            Object value = r[b.inputFieldIndex];
            Object out = b.processor.convert(value);
            cache[b.bindingIndex] = out;
        }
        return cache;
    }

    /**
     * 往流中塞值
     *
     * @param row
     * @param cache
     * @return
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    private Object[] setRowValue(Object[] row, Object[] cache) throws SQLException, UnsupportedEncodingException {
        int count = inputRowLength + newFieldLength;
        Object[] output = new Object[count];
        System.arraycopy(row, 0, output, 0, inputRowLength);
        for (int i = 0; i < newFieldLength; i++) {
            output[inputRowLength + i] = dealResultDataType(cache[i], i);
        }
        return output;
    }

    private Object dealResultDataType(Object data, int index) {
        ValueMetaInterface valueMetaInterface = this.data.outputRowMeta.getValueMetaList().get(inputRowLength + index);
        int type = valueMetaInterface.getType();
        return dealDataType(type, data);
    }

    /**
     * 获取常用数据类型
     *
     * @param type
     * @return
     */
    public Object dealDataType(int type, Object data) {
        if (data == null) return null;
        switch (type) {
            case 1:
//                return Double.valueOf(data.toString());
                return new BigDecimal(data.toString()).doubleValue();
            case 2:
                return String.valueOf(data);
            case 4:
                return Boolean.valueOf(data.toString());
            case 5:
                return Long.valueOf(data.toString());
            case 6:
                return BigDecimal.valueOf(Long.parseLong(data.toString()));
            case 7: // Serializable
                return data;
            case 8:
                return Byte.valueOf(data.toString());
            case 9:
                return Long.valueOf(data.toString());
            case 0:
            case 3:
            case 10:
            default:
                return data;
        }
    }


    /**
     * 步骤组件执行完成（不管成功还是失败，或者异常）后，设计器调用此方法释放资源。譬如：释放文件句柄、数据库连接等
     */
    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
        logDebug("Groovy Step Dispose ...");
    }

}
