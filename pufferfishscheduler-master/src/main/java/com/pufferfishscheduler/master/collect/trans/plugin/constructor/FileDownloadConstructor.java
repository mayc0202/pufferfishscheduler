package com.pufferfishscheduler.master.collect.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.bean.PufferfishSchedulerApplicationContext;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.collect.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.master.collect.trans.plugin.StepContext;
import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import com.pufferfishscheduler.plugin.FileDownloadStepMeta;
import com.pufferfishscheduler.plugin.common.Param;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Optional;

/**
 * 文件下载插件构造器
 * 负责将JSON配置转换为FileDownloadStepMeta对象
 */
public class FileDownloadConstructor extends AbstractStepMetaConstructor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDownloadConstructor.class);

    // 配置键常量
    private static final String KEY_NAME = "name";
    private static final String KEY_DATA = "data";
    private static final String KEY_URL = "url";
    private static final String KEY_REQUEST_METHOD = "requestMethod";
    private static final String KEY_REQUEST_TYPE = "requestType";
    private static final String KEY_FILE_PATH = "filePath";
    private static final String KEY_RETRY_NUM = "retryNum";
    private static final String KEY_RETRY_TIME = "retryTime";
    private static final String KEY_FILE_NAME = "fileName";
    private static final String KEY_HEADER_LIST = "headerList";
    private static final String KEY_USE_XFORM = "useXForm";
    private static final String KEY_XFORM_LIST = "xFormList";
    private static final String KEY_USE_RAW = "useRaw";
    private static final String KEY_RAW = "raw";
    private static final String KEY_COPIES_CACHE = "copiesCache";
    private static final String KEY_DISTRIBUTE_TYPE = "distributeType";

    // 常量值
    private static final String POST_METHOD = "POST";
    private static final boolean DEFAULT_USE_XFORM = true;
    private static final boolean DEFAULT_USE_RAW = false;

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        validateInput(config, context);

        JSONObject jsonObject = parseConfig(config);
        String componentName = extractComponentName(jsonObject);
        JSONObject data = extractData(jsonObject);

        boolean validateMode = context.isValidate();

        FileDownloadStepMeta stepMeta = buildStepMeta(data, componentName, validateMode);
        StepMeta targetStepMeta = buildStepMeta(context, stepMeta, componentName, data);

        applyStepConfiguration(targetStepMeta, data);

        LOGGER.debug("File Download step created successfully: {}", componentName);
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
    private FileDownloadStepMeta buildStepMeta(JSONObject data, String componentName, boolean validateMode) {
        FileDownloadStepMeta stepMeta = new FileDownloadStepMeta();
        stepMeta.setDefault();

        // 基础配置
        configureBasicSettings(stepMeta, data, componentName, validateMode);

        // 重试配置
        configureRetrySettings(stepMeta, data);

        // 请求配置
        configureRequestSettings(stepMeta, data);

        // 请求体配置
        configureRequestBody(stepMeta, data);

        return stepMeta;
    }

    /**
     * 配置基础设置
     */
    private void configureBasicSettings(FileDownloadStepMeta stepMeta, JSONObject data,
                                        String componentName, boolean validateMode) {
        // URL
        String url = data.getString(KEY_URL);
        validateIfNeeded(url, componentName, "URL", validateMode);
        stepMeta.setUrl(url);

        // 请求方式
        String requestMethod = data.getString(KEY_REQUEST_METHOD);
        validateIfNeeded(requestMethod, componentName, "请求方法", validateMode);
        stepMeta.setRequestMethod(requestMethod);

        // 请求类型（仅POST需要校验）
        String requestType = data.getString(KEY_REQUEST_TYPE);
        if (validateMode && POST_METHOD.equals(requestMethod)) {
            validateIfNeeded(requestType, componentName, "请求类型", true);
        }
        stepMeta.setRequestType(requestType);

        // 文件保存路径
        String filePath = data.getString(KEY_FILE_PATH);
        validateIfNeeded(filePath, componentName, "附件保存目录", validateMode);
        String fullPath = buildFullFilePath(filePath);
        createOutputDir(fullPath);
        stepMeta.setFilePath(fullPath);

        // 文件名
        Optional.ofNullable(data.getString(KEY_FILE_NAME))
                .ifPresent(stepMeta::setFileName);
    }

    /**
     * 构建完整文件路径
     */
    private String buildFullFilePath(String relativePath) {
        FilePathConfig filePathConfig = PufferfishSchedulerApplicationContext.getBean(FilePathConfig.class);
        String rootPath = filePathConfig.getLocalPath();
        return rootPath + File.separator + relativePath;
    }

    /**
     * 配置重试设置
     */
    private void configureRetrySettings(FileDownloadStepMeta stepMeta, JSONObject data) {
        Optional.ofNullable(data.getInteger(KEY_RETRY_NUM))
                .ifPresent(stepMeta::setRetryNum);
        Optional.ofNullable(data.getLong(KEY_RETRY_TIME))
                .ifPresent(stepMeta::setRetryTime);
    }

    /**
     * 配置请求设置
     */
    private void configureRequestSettings(FileDownloadStepMeta stepMeta, JSONObject data) {
        // 请求头
        JSONArray headerList = data.getJSONArray(KEY_HEADER_LIST);
        if (headerList != null && !headerList.isEmpty()) {
            List<Param> params = headerList.toJavaList(Param.class);
            stepMeta.setParams(params);
        }
    }

    /**
     * 配置请求体设置
     */
    private void configureRequestBody(FileDownloadStepMeta stepMeta, JSONObject data) {
        // Form表单
        boolean useXForm = data.getBoolean(KEY_USE_XFORM) != null ?
                data.getBoolean(KEY_USE_XFORM) : DEFAULT_USE_XFORM;
        stepMeta.setUseXForm(useXForm);

        if (useXForm) {
            JSONArray xFormList = data.getJSONArray(KEY_XFORM_LIST);
            if (xFormList != null && !xFormList.isEmpty()) {
                List<Param> xForms = xFormList.toJavaList(Param.class);
                stepMeta.setxFormParams(xForms);
            }
        }

        // Raw请求体
        boolean useRaw = data.getBoolean(KEY_USE_RAW) != null ?
                data.getBoolean(KEY_USE_RAW) : DEFAULT_USE_RAW;
        stepMeta.setUseRaw(useRaw);
        Optional.ofNullable(data.getString(KEY_RAW)).ifPresent(stepMeta::setRaw);
    }

    /**
     * 条件校验
     */
    private void validateIfNeeded(String value, String componentName, String fieldName, boolean validateMode) {
        if (validateMode) {
            validateBlank(value, "【" + componentName + "】", fieldName);
        }
    }

    /**
     * 构建步骤元数据对象
     */
    private StepMeta buildStepMeta(StepContext context, FileDownloadStepMeta stepMeta,
                                   String componentName, JSONObject data) {
        String pluginId = context.getRegistryID().getPluginId(StepPluginType.class, stepMeta);

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
}