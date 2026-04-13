package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.vo.collect.ExcelOutputFieldVo;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransParam;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.excelwriter.ExcelWriterStepField;
import org.pentaho.di.trans.steps.excelwriter.ExcelWriterStepMeta;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Excel输出构造器
 */
public class ExcelOutputConstructor extends AbstractStepMetaConstructor {

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        // 验证输入参数
        validateInput(config, context);

        // 从应用上下文获取文件路径配置
        FilePathConfig filePathConfig = PufferfishSchedulerApplicationContext.getBean(FilePathConfig.class);

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

        // 输出文件目录，对应FTP的文件目录
        String outputPath = data.getString("outputPath");
        if (context.isValidate()) {
            validateBlank(outputPath, "【" + name + "】", "输出文件目录");
        }
        // 文件名称，注意需要限制一下不要填写那些特殊字符
        String fileName = data.getString("fileName");
        if (context.isValidate()) {
            validateBlank(fileName, "【" + name + "】", "文件名称");
        }

        String fileSourceType = data.getString("fileSourceType");

        // 文件扩展名
        String extension = data.getString("extension");
        if (StringUtils.isBlank(extension)) {
            extension = "xls";
        }
        // 如果文件已存在, new-覆盖原文件；reuse-使用现有的文件输出
        String ifFileExists = data.getString("ifFileExists");
        if (StringUtils.isBlank(ifFileExists)) {
            ifFileExists = ExcelWriterStepMeta.IF_FILE_EXISTS_CREATE_NEW;
        }
        // 分割数据行
        int splitEvery = data.getIntValue("splitEvery");
        // 采用流式写入
        boolean streamingData = data.getBooleanValue("streamingData");
        // 文件名中包含日期
        boolean dateInFilename = data.getBooleanValue("dateInFilename");

        // 日期时间格式 date_time_format
        String dateTimeFormat = data.getString("dateTimeFormat");
        // 工作表名称
        String sheetname = data.getString("sheetname");
        if (StringUtils.isBlank(sheetname)) {
            sheetname = "Sheet1";
        }
        // 数据输出起始单元格
        String startingCell = data.getString("startingCell");
        if (StringUtils.isBlank(startingCell)) {
            startingCell = "A1";
        }
        // 追加行
        boolean appendLines = data.getBooleanValue("appendLines");
        // 删除头部
        boolean appendOmitHeader = data.getBooleanValue("appendOmitHeader");
        // 字段
        List<ExcelOutputFieldVo> outputFields = new ArrayList<>();
        JSONArray outputFieldsArray = data.getJSONArray("fieldList");
        if (outputFieldsArray != null) {
            outputFields = outputFieldsArray.toJavaList(ExcelOutputFieldVo.class);
        }

        /**
         * 2. 设置kettle对应组件的属性
         */
        ExcelWriterStepMeta excelWriterStepMeta = new ExcelWriterStepMeta();

        if (Constants.FILE_SOURCE_TYPE.FTP_FILE.equals(fileSourceType)) {
            //输出到临时目录
            String stepId = context.getId();
            String fullPath = buildLocalFullPath(context.getFlowId(), stepId, fileName);
            createOutputDir(fullPath);
            excelWriterStepMeta.setFileName(fullPath);
        } else {
            String fullPath = filePathConfig.getLocalPath() + File.separator + outputPath + File.separator + fileName;
            excelWriterStepMeta.setFileName(fullPath);
        }

        excelWriterStepMeta.setHeaderEnabled(true);
        excelWriterStepMeta.setExtension(extension);
        excelWriterStepMeta.setIfFileExists(ifFileExists);
        excelWriterStepMeta.setSplitEvery(splitEvery);
        excelWriterStepMeta.setStreamingData(streamingData);
        excelWriterStepMeta.setDateInFilename(dateInFilename);
        //指定日期时间格式
        if (dateInFilename && StringUtils.isNotBlank(dateTimeFormat)) {
            excelWriterStepMeta.setSpecifyFormat(true);
            excelWriterStepMeta.setDateTimeFormat(dateTimeFormat);
        } else {
            excelWriterStepMeta.setSpecifyFormat(false);
        }
        //设为活动工作表
        excelWriterStepMeta.setMakeSheetActive(true);
        excelWriterStepMeta.setSheetname(sheetname);
        excelWriterStepMeta.setStartingCell(startingCell);
        excelWriterStepMeta.setAppendLines(appendLines);
        excelWriterStepMeta.setAppendOmitHeader(appendOmitHeader);
        excelWriterStepMeta.setOutputFields(buildFieldArray(outputFields));

        // 从插件注册表中获取Excel输入插件的插件ID
        String eiPluginId = context.getRegistryID().getPluginId(StepPluginType.class, excelWriterStepMeta);
        StepMeta stepMeta = context.getStepMetaMap().get(context.getId());
        if (null == stepMeta) {
            stepMeta = new StepMeta(eiPluginId, name, excelWriterStepMeta);
        } else {
            stepMeta.setStepID(eiPluginId);
            stepMeta.setName(name);
            stepMeta.setStepMetaInterface(excelWriterStepMeta);
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

    /**
     * 字段列表类型转换
     *
     * @param fieldList
     * @return
     */
    private ExcelWriterStepField[] buildFieldArray(List<ExcelOutputFieldVo> fieldList) {
        ExcelWriterStepField[] outputFields = new ExcelWriterStepField[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++) {
            ExcelOutputFieldVo vo = fieldList.get(i);
            ExcelWriterStepField f = new ExcelWriterStepField();
            f.setName(vo.getName());
            f.setTitle(vo.getTitle());
            f.setType(ValueMetaFactory.getValueMetaName(StringUtils.isBlank(vo.getType()) ? ValueMetaInterface.TYPE_STRING : Integer.parseInt(vo.getType())));
            f.setFormat(vo.getFormat());
            f.setHyperlinkField(vo.getHyperlinkField());
            outputFields[i] = f;
        }
        return outputFields;
    }

    /**
     * 执行前处理
     */
    @Override
    public void beforeStep(Integer flowId, String stepId, String stepConfig, List<TransParam> params) {
        if (null == stepId) {
            return;
        }
        super.beforeGenerateLocalPath(flowId, stepId);
    }

    /**
     * 执行后处理
     */
    @Override
    public void afterStep(Integer flowId, String stepId, String stepConfig) {
        super.afterStepForFileOutputComponent(flowId, stepId, stepConfig);
    }

}
