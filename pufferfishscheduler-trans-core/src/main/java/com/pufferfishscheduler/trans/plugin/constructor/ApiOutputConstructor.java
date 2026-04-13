package com.pufferfishscheduler.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepContext;
import com.pufferfishscheduler.plugin.ApiOutputStepMeta;
import com.pufferfishscheduler.plugin.common.Param;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * API输出步骤构造器
 * 负责将JSON配置转换为ApiOutputStepMeta对象
 */
public class ApiOutputConstructor extends AbstractStepMetaConstructor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiOutputConstructor.class);

    // 配置键常量
    private static final String KEY_NAME = "name";
    private static final String KEY_DATA = "data";
    private static final String KEY_URL = "url";
    private static final String KEY_REQUEST_METHOD = "requestMethod";
    private static final String KEY_REQUEST_TYPE = "requestType";
    private static final String KEY_RESPONSE_STATUS = "responseStatus";
    private static final String KEY_RESPONSE_HEADER = "responseHeader";
    private static final String KEY_RESPONSE_BODY = "responseBody";
    private static final String KEY_HEADER_LIST = "headerList";
    private static final String KEY_REQUEST_CODE = "requestCode";
    private static final String KEY_RESPONSE_CODE = "responseCode";
    private static final String KEY_CONNECT_TIMEOUT = "connectOutTime";
    private static final String KEY_READ_TIMEOUT = "readOutTime";
    private static final String KEY_RETRY_NUM = "retryNum";
    private static final String KEY_RETRY_TIME = "retryTime";
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

        ApiOutputStepMeta stepMeta = buildStepMeta(data, componentName, validateMode);
        StepMeta targetStepMeta = buildStepMeta(context, stepMeta, componentName, data);

        applyStepConfiguration(targetStepMeta, data);

        LOGGER.debug("API Output step created successfully: {}", componentName);
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
    private ApiOutputStepMeta buildStepMeta(JSONObject data, String componentName, boolean validateMode) {
        ApiOutputStepMeta stepMeta = new ApiOutputStepMeta();
        stepMeta.setDefault();

        // 基础配置
        configureBasicSettings(stepMeta, data, componentName, validateMode);

        // 高级配置
        configureAdvancedSettings(stepMeta, data);

        // 请求体配置
        configureRequestBody(stepMeta, data);

        return stepMeta;
    }

    /**
     * 配置基础设置
     */
    private void configureBasicSettings(ApiOutputStepMeta stepMeta, JSONObject data,
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

        // 响应字段名
        String responseStatus = data.getString(KEY_RESPONSE_STATUS);
        validateIfNeeded(responseStatus, componentName, "响应状态码字段名", validateMode);
        stepMeta.setResponseStatus(responseStatus);

        String responseHeader = data.getString(KEY_RESPONSE_HEADER);
        validateIfNeeded(responseHeader, componentName, "响应头字段名", validateMode);
        stepMeta.setResponseHeader(responseHeader);

        String responseBody = data.getString(KEY_RESPONSE_BODY);
        validateIfNeeded(responseBody, componentName, "响应体字段名", validateMode);
        stepMeta.setResponseBody(responseBody);
    }

    /**
     * 配置高级设置
     */
    private void configureAdvancedSettings(ApiOutputStepMeta stepMeta, JSONObject data) {
        // 编码设置
        Optional.ofNullable(data.getString(KEY_REQUEST_CODE))
                .ifPresent(stepMeta::setRequestCode);
        Optional.ofNullable(data.getString(KEY_RESPONSE_CODE))
                .ifPresent(stepMeta::setResponseCode);

        // 超时设置
        Optional.ofNullable(data.getInteger(KEY_CONNECT_TIMEOUT))
                .ifPresent(stepMeta::setConnectOutTime);
        Optional.ofNullable(data.getInteger(KEY_READ_TIMEOUT))
                .ifPresent(stepMeta::setReadOutTime);

        // 重试设置
        Optional.ofNullable(data.getInteger(KEY_RETRY_NUM))
                .ifPresent(stepMeta::setRetryNum);
        Optional.ofNullable(data.getLong(KEY_RETRY_TIME))
                .ifPresent(stepMeta::setRetryTime);

        // 请求头
        configureHeaders(stepMeta, data);
    }

    /**
     * 配置请求头
     */
    private void configureHeaders(ApiOutputStepMeta stepMeta, JSONObject data) {
        JSONArray headerList = data.getJSONArray(KEY_HEADER_LIST);
        if (headerList != null && !headerList.isEmpty()) {
            List<Param> params = headerList.toJavaList(Param.class);
            stepMeta.setParams(params);
        }
    }

    /**
     * 配置请求体设置
     */
    private void configureRequestBody(ApiOutputStepMeta stepMeta, JSONObject data) {
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
    private StepMeta buildStepMeta(StepContext context, ApiOutputStepMeta stepMeta,
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