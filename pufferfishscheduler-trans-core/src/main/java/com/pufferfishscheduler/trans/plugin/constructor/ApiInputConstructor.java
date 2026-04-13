package com.pufferfishscheduler.trans.plugin.constructor;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.ApiInputStepMeta;
import com.pufferfishscheduler.plugin.common.Param;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.trans.plugin.AbstractStepMetaConstructor;
import com.pufferfishscheduler.trans.plugin.StepContext;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * API输入步骤构造器
 * 负责将JSON配置转换为ApiInputStepMeta对象
 */
public class ApiInputConstructor extends AbstractStepMetaConstructor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiInputConstructor.class);

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
    private static final String KEY_USE_PAGE = "usePage";
    private static final String KEY_PAGE_CONDITION = "pageCondition";
    private static final String KEY_START_PAGE_NO = "startPageNo";
    private static final String KEY_COPIES_CACHE = "copiesCache";
    private static final String KEY_DISTRIBUTE_TYPE = "distributeType";

    // 常量值
    private static final String POST_METHOD = "POST";
    private static final String PAGE_NO_VARIABLE = "${page_no}";
    private static final long DEFAULT_START_PAGE_NO = 1L;
    private static final boolean DEFAULT_USE_XFORM = true;
    private static final boolean DEFAULT_USE_RAW = false;
    private static final boolean DEFAULT_USE_PAGE = false;

    @Override
    public StepMeta create(String config, TransMeta transMeta, StepContext context) {
        validateInput(config, context);

        JSONObject jsonObject = parseConfig(config);
        String componentName = extractComponentName(jsonObject);
        JSONObject data = extractData(jsonObject);

        boolean validateMode = context.isValidate();

        ApiInputStepMeta stepMeta = buildStepMeta(data, componentName, validateMode);
        StepMeta targetStepMeta = buildStepMeta(context, stepMeta, componentName, data);

        applyStepConfiguration(targetStepMeta, data);

        LOGGER.debug("API Input step created successfully: {}", componentName);
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
    private ApiInputStepMeta buildStepMeta(JSONObject data, String componentName, boolean validateMode) {
        ApiInputStepMeta stepMeta = new ApiInputStepMeta();
        stepMeta.setDefault();

        // 基础配置
        configureBasicSettings(stepMeta, data, componentName, validateMode);

        // 高级配置
        configureAdvancedSettings(stepMeta, data);

        // 分页配置
        configurePaginationSettings(stepMeta, data);

        return stepMeta;
    }

    /**
     * 配置基础设置
     */
    private void configureBasicSettings(ApiInputStepMeta stepMeta, JSONObject data,
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
    private void configureAdvancedSettings(ApiInputStepMeta stepMeta, JSONObject data) {
        // 请求头
        JSONArray headerList = data.getJSONArray(KEY_HEADER_LIST);
        if (headerList != null && !headerList.isEmpty()) {
            List<Param> params = headerList.toJavaList(Param.class);
            stepMeta.setParams(params);
        }

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

        // 请求体设置
        configureRequestBody(stepMeta, data);
    }

    /**
     * 配置请求体设置
     */
    private void configureRequestBody(ApiInputStepMeta stepMeta, JSONObject data) {
        // Form表单
        boolean useXForm = data.getBoolean(KEY_USE_XFORM) != null ?
                data.getBoolean(KEY_USE_XFORM) : DEFAULT_USE_XFORM;
        stepMeta.setUseXForm(useXForm);

        List<String> variableList = new ArrayList<>();
        if (useXForm) {
            JSONArray xFormList = data.getJSONArray(KEY_XFORM_LIST);
            if (xFormList != null && !xFormList.isEmpty()) {
                List<Param> xForms = xFormList.toJavaList(Param.class);
                variableList = xForms.stream().map(Param::getValue).toList();
                stepMeta.setxFormParams(xForms);
            }
        }

        // Raw请求体
        boolean useRaw = data.getBoolean(KEY_USE_RAW) != null ?
                data.getBoolean(KEY_USE_RAW) : DEFAULT_USE_RAW;
        stepMeta.setUseRaw(useRaw);
        Optional.ofNullable(data.getString(KEY_RAW)).ifPresent(stepMeta::setRaw);

        // 保存变量列表供分页校验使用
        data.put("_variableList", variableList);
    }

    /**
     * 配置分页设置
     */
    private void configurePaginationSettings(ApiInputStepMeta stepMeta, JSONObject data) {
        boolean usePage = data.getBoolean(KEY_USE_PAGE) != null ?
                data.getBoolean(KEY_USE_PAGE) : DEFAULT_USE_PAGE;
        stepMeta.setUsePage(usePage);

        Optional.ofNullable(data.getString(KEY_PAGE_CONDITION))
                .ifPresent(stepMeta::setPageCondition);

        if (usePage) {
            Long startPageNo = data.getLong(KEY_START_PAGE_NO);
            if (startPageNo == null || startPageNo <= 0) {
                startPageNo = DEFAULT_START_PAGE_NO;
            }
            stepMeta.setStartPageNo(startPageNo);

            validatePageVariable(stepMeta, data);
        }
    }

    /**
     * 校验分页变量
     */
    private void validatePageVariable(ApiInputStepMeta stepMeta, JSONObject data) {
        String url = stepMeta.getUrl();
        String raw = stepMeta.getRaw();

        @SuppressWarnings("unchecked")
        List<String> variableList = (List<String>) data.get("_variableList");

        boolean hasPageVariable = (url != null && url.contains(PAGE_NO_VARIABLE)) ||
                (raw != null && raw.contains(PAGE_NO_VARIABLE)) ||
                (variableList != null && variableList.stream().anyMatch(v -> v != null && v.contains(PAGE_NO_VARIABLE)));

        if (!hasPageVariable) {
            throw new BusinessException("分页查询需要配合使用${page_no}变量，当前配置中缺少相关变量！");
        }
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
    private StepMeta buildStepMeta(StepContext context, ApiInputStepMeta stepMeta,
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