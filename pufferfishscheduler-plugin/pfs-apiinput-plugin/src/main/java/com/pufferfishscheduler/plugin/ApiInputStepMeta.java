package com.pufferfishscheduler.plugin;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.pufferfishscheduler.plugin.common.Param;
import com.pufferfishscheduler.plugin.common.WayType;
import org.apache.commons.beanutils.BeanUtils;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

/**
 * API输入步骤元数据类
 * 用于配置HTTP API输入参数，支持分页获取数据
 */
@Step(
        id = "ApiInput",
        name = "API输入",
        description = "使用GET/POST方式，调用API接口获取输入数据。",
        image = "APIInput.svg",
        categoryDescription = "输入"
)
public class ApiInputStepMeta extends BaseStepMeta implements StepMetaInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiInputStepMeta.class);

    // XML标签常量
    private static final String XML_TAG_URL = "url";
    private static final String XML_TAG_REQUEST_METHOD = "requestMethod";
    private static final String XML_TAG_REQUEST_TYPE = "requestType";
    private static final String XML_TAG_USE_XFORM = "useXForm";
    private static final String XML_TAG_USE_RAW = "useRaw";
    private static final String XML_TAG_USE_PAGE = "usePage";
    private static final String XML_TAG_RAW = "raw";
    private static final String XML_TAG_PAGE_CONDITION = "pageCondition";
    private static final String XML_TAG_RESPONSE_STATUS = "responseStatus";
    private static final String XML_TAG_RESPONSE_HEADER = "responseHeader";
    private static final String XML_TAG_RESPONSE_BODY = "responseBody";
    private static final String XML_TAG_INTERVAL_FROM = "intervalFrom";
    private static final String XML_TAG_INTERVAL_TO = "intervalTo";
    private static final String XML_TAG_REQUEST_CODE = "requestCode";
    private static final String XML_TAG_RESPONSE_CODE = "responseCode";
    private static final String XML_TAG_CONNECT_TIMEOUT = "connectOutTime";
    private static final String XML_TAG_READ_TIMEOUT = "readOutTime";
    private static final String XML_TAG_RETRY_NUM = "retryNum";
    private static final String XML_TAG_RETRY_TIME = "retryTime";
    private static final String XML_TAG_START_PAGE_NO = "startPageNo";
    private static final String XML_TAG_PARAMS = "params";
    private static final String XML_TAG_PARAM = "param";
    private static final String XML_TAG_XFORMS = "xforms";
    private static final String XML_TAG_XFORM = "xform";
    private static final String XML_TAG_NAME = "name";
    private static final String XML_TAG_VALUE = "value";
    private static final String XML_TAG_DESCRIPTION = "description";

    // 仓库属性前缀常量
    private static final String REPO_ATTR_PARAM_NAME = "param_name";
    private static final String REPO_ATTR_PARAM_VALUE = "param_value";
    private static final String REPO_ATTR_PARAM_DESCRIPTION = "param_description";
    private static final String REPO_ATTR_XFORM_NAME = "xform_name";
    private static final String REPO_ATTR_XFORM_VALUE = "xform_value";
    private static final String REPO_ATTR_XFORM_DESCRIPTION = "xform_description";

    // 默认值常量
    private static final String DEFAULT_URL = "";
    private static final String DEFAULT_REQUEST_TYPE = "";
    private static final String DEFAULT_RAW = "";
    private static final String DEFAULT_PAGE_CONDITION = "";
    private static final String DEFAULT_RESPONSE_STATUS = "in_response_status";
    private static final String DEFAULT_RESPONSE_HEADER = "in_response_header";
    private static final String DEFAULT_RESPONSE_BODY = "in_response_body";
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final int DEFAULT_TIMEOUT_MS = 30000;
    private static final int DEFAULT_RETRY_NUM = 2;
    private static final long DEFAULT_RETRY_TIME_MS = 10000L;
    private static final long DEFAULT_INTERVAL_FROM_MS = 1000L;
    private static final long DEFAULT_INTERVAL_TO_MS = 5000L;
    private static final long DEFAULT_START_PAGE_NO = 1L;
    private static final String YES_VALUE = "Y";

    // 配置属性
    private String url;
    private String requestMethod;
    private String requestType;
    private List<Param> params = new ArrayList<>();
    private boolean useXForm = true;
    private List<Param> xFormParams = new ArrayList<>();
    private boolean useRaw;
    private String raw;
    private boolean usePage;
    private String pageCondition;
    private String requestCode;
    private String responseCode;
    private Integer connectOutTime;
    private Integer readOutTime;
    private String responseStatus;
    private String responseHeader;
    private String responseBody;
    private Long intervalFrom = DEFAULT_INTERVAL_FROM_MS;
    private Long intervalTo = DEFAULT_INTERVAL_TO_MS;
    private Integer retryNum = DEFAULT_RETRY_NUM;
    private Long retryTime = DEFAULT_RETRY_TIME_MS;
    private Long startPageNo = DEFAULT_START_PAGE_NO;

    public ApiInputStepMeta() {
        setDefault();
    }

    @Override
    public void setDefault() {
        this.url = DEFAULT_URL;
        this.requestMethod = WayType.GET.getDescription();
        this.requestType = DEFAULT_REQUEST_TYPE;
        this.params = new ArrayList<>();
        this.useXForm = true;
        this.xFormParams = new ArrayList<>();
        this.useRaw = false;
        this.raw = DEFAULT_RAW;
        this.usePage = false;
        this.pageCondition = DEFAULT_PAGE_CONDITION;
        this.responseStatus = DEFAULT_RESPONSE_STATUS;
        this.responseHeader = DEFAULT_RESPONSE_HEADER;
        this.responseBody = DEFAULT_RESPONSE_BODY;
        this.requestCode = DEFAULT_ENCODING;
        this.responseCode = DEFAULT_ENCODING;
        this.connectOutTime = DEFAULT_TIMEOUT_MS;
        this.readOutTime = DEFAULT_TIMEOUT_MS;
        this.intervalFrom = DEFAULT_INTERVAL_FROM_MS;
        this.intervalTo = DEFAULT_INTERVAL_TO_MS;
        this.retryNum = DEFAULT_RETRY_NUM;
        this.retryTime = DEFAULT_RETRY_TIME_MS;
        this.startPageNo = DEFAULT_START_PAGE_NO;
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                                 int copyNr, TransMeta transMeta, Trans trans) {
        return new ApiInputStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new ApiInputStepData();
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder xml = new StringBuilder(1024);

        // 基本属性
        appendXmlTag(xml, XML_TAG_URL, url);
        appendXmlTag(xml, XML_TAG_REQUEST_METHOD, requestMethod);
        appendXmlTag(xml, XML_TAG_REQUEST_TYPE, requestType);
        appendXmlTag(xml, XML_TAG_USE_XFORM, useXForm);
        appendXmlTag(xml, XML_TAG_USE_RAW, useRaw);
        appendXmlTag(xml, XML_TAG_USE_PAGE, usePage);
        appendXmlTag(xml, XML_TAG_RAW, raw);
        appendXmlTag(xml, XML_TAG_PAGE_CONDITION, pageCondition);
        appendXmlTag(xml, XML_TAG_RESPONSE_STATUS, responseStatus);
        appendXmlTag(xml, XML_TAG_RESPONSE_HEADER, responseHeader);
        appendXmlTag(xml, XML_TAG_RESPONSE_BODY, responseBody);
        appendXmlTag(xml, XML_TAG_INTERVAL_FROM, intervalFrom);
        appendXmlTag(xml, XML_TAG_INTERVAL_TO, intervalTo);
        appendXmlTag(xml, XML_TAG_REQUEST_CODE, requestCode);
        appendXmlTag(xml, XML_TAG_RESPONSE_CODE, responseCode);
        appendXmlTag(xml, XML_TAG_CONNECT_TIMEOUT, connectOutTime);
        appendXmlTag(xml, XML_TAG_READ_TIMEOUT, readOutTime);
        appendXmlTag(xml, XML_TAG_RETRY_NUM, retryNum);
        appendXmlTag(xml, XML_TAG_RETRY_TIME, retryTime);
        appendXmlTag(xml, XML_TAG_START_PAGE_NO, startPageNo);

        // 参数列表
        appendParamListXml(xml, XML_TAG_PARAMS, XML_TAG_PARAM, params);

        // XForm参数列表
        appendParamListXml(xml, XML_TAG_XFORMS, XML_TAG_XFORM, xFormParams);

        return xml.toString();
    }

    private void appendXmlTag(StringBuilder xml, String tag, String value) {
        xml.append("    ").append(XMLHandler.addTagValue(tag, value != null ? value : ""));
    }

    private void appendXmlTag(StringBuilder xml, String tag, boolean value) {
        xml.append("    ").append(XMLHandler.addTagValue(tag, value));
    }

    private void appendXmlTag(StringBuilder xml, String tag, Integer value) {
        if (value == null) {
            xml.append("    ").append(XMLHandler.addTagValue(tag, ""));
        } else {
            xml.append("    ").append(XMLHandler.addTagValue(tag, value));
        }
    }

    private void appendXmlTag(StringBuilder xml, String tag, Long value) {
        if (value == null) {
            xml.append("    ").append(XMLHandler.addTagValue(tag, ""));
        } else {
            xml.append("    ").append(XMLHandler.addTagValue(tag, value));
        }
    }

    private void appendParamListXml(StringBuilder xml, String listTag, String itemTag,
                                    List<Param> paramList) {
        xml.append("    <").append(listTag).append(">").append(Const.CR);

        for (Param param : paramList) {
            xml.append("      <").append(itemTag).append(">").append(Const.CR);
            xml.append("        ").append(XMLHandler.addTagValue(XML_TAG_NAME, param.getName()));
            xml.append("        ").append(XMLHandler.addTagValue(XML_TAG_VALUE, param.getValue()));
            xml.append("        ").append(XMLHandler.addTagValue(XML_TAG_DESCRIPTION,
                    param.getDescription(), false));
            xml.append("      </").append(itemTag).append(">").append(Const.CR);
        }

        xml.append("    </").append(listTag).append(">").append(Const.CR);
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore,
                        ObjectId transformationId, ObjectId stepId) throws KettleException {
        try {
            saveBasicAttributes(rep, transformationId, stepId);
            saveParamListToRepository(rep, transformationId, stepId, params,
                    REPO_ATTR_PARAM_NAME, REPO_ATTR_PARAM_VALUE, REPO_ATTR_PARAM_DESCRIPTION);
            saveParamListToRepository(rep, transformationId, stepId, xFormParams,
                    REPO_ATTR_XFORM_NAME, REPO_ATTR_XFORM_VALUE, REPO_ATTR_XFORM_DESCRIPTION);
        } catch (Exception e) {
            throw new KettleException("Unable to save step into repository: " + stepId, e);
        }
    }

    private void saveBasicAttributes(Repository rep, ObjectId transformationId, ObjectId stepId)
            throws KettleException {
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_URL, url);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_REQUEST_METHOD, requestMethod);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_REQUEST_TYPE, requestType);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_USE_XFORM, useXForm);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_USE_RAW, useRaw);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_USE_PAGE, usePage);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RAW, raw);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_PAGE_CONDITION, pageCondition);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RESPONSE_STATUS, responseStatus);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RESPONSE_HEADER, responseHeader);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RESPONSE_BODY, responseBody);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_INTERVAL_FROM, intervalFrom);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_INTERVAL_TO, intervalTo);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_REQUEST_CODE, requestCode);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RESPONSE_CODE, responseCode);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_CONNECT_TIMEOUT, connectOutTime);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_READ_TIMEOUT, readOutTime);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RETRY_NUM, retryNum);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RETRY_TIME, retryTime);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_START_PAGE_NO, startPageNo);
    }

    private void saveParamListToRepository(Repository rep, ObjectId transformationId, ObjectId stepId,
                                           List<Param> paramList, String nameAttr,
                                           String valueAttr, String descAttr) throws KettleException {
        for (int i = 0; i < paramList.size(); i++) {
            Param param = paramList.get(i);
            rep.saveStepAttribute(transformationId, stepId, i, nameAttr, param.getName());
            rep.saveStepAttribute(transformationId, stepId, i, valueAttr, param.getValue());
            rep.saveStepAttribute(transformationId, stepId, i, descAttr, param.getDescription());
        }
    }

    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore)
            throws KettleXMLException {
        try {
            loadBasicAttributesFromXml(stepNode);
            loadParamListFromXml(stepNode, XML_TAG_PARAMS, XML_TAG_PARAM, params = new ArrayList<>());
            loadParamListFromXml(stepNode, XML_TAG_XFORMS, XML_TAG_XFORM, xFormParams = new ArrayList<>());
        } catch (Exception e) {
            throw new KettleXMLException("API plugin unable to read step info from XML node", e);
        }
    }

    private void loadBasicAttributesFromXml(Node stepNode) {
        url = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_URL));
        requestMethod = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_REQUEST_METHOD));
        requestType = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_REQUEST_TYPE));
        useXForm = YES_VALUE.equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_USE_XFORM)));
        useRaw = YES_VALUE.equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_USE_RAW)));
        usePage = YES_VALUE.equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_USE_PAGE)));
        raw = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_RAW));
        pageCondition = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_PAGE_CONDITION));
        responseStatus = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_RESPONSE_STATUS));
        responseHeader = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_RESPONSE_HEADER));
        responseBody = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_RESPONSE_BODY));
        requestCode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_REQUEST_CODE));
        responseCode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_RESPONSE_CODE));

        intervalFrom = parseLongSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_INTERVAL_FROM)));
        intervalTo = parseLongSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_INTERVAL_TO)));
        connectOutTime = parseIntegerSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_CONNECT_TIMEOUT)));
        readOutTime = parseIntegerSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_READ_TIMEOUT)));
        retryNum = parseIntegerSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_RETRY_NUM)));
        retryTime = parseLongSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_RETRY_TIME)));
        startPageNo = parseLongSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_START_PAGE_NO)));
    }

    private void loadParamListFromXml(Node stepNode, String listTag, String itemTag,
                                      List<Param> targetList) {
        Node listNode = XMLHandler.getSubNode(stepNode, listTag);
        int itemCount = XMLHandler.countNodes(listNode, itemTag);

        for (int i = 0; i < itemCount; i++) {
            Node itemNode = XMLHandler.getSubNodeByNr(listNode, itemTag, i);
            targetList.add(new Param(
                    XMLHandler.getTagValue(itemNode, XML_TAG_NAME),
                    XMLHandler.getTagValue(itemNode, XML_TAG_VALUE),
                    XMLHandler.getTagValue(itemNode, XML_TAG_DESCRIPTION)
            ));
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore,
                        ObjectId stepId, List<DatabaseMeta> databases) throws KettleException {
        try {
            loadBasicAttributesFromRepository(rep, stepId);
            loadParamListFromRepository(rep, stepId, params = new ArrayList<>(),
                    REPO_ATTR_PARAM_NAME, REPO_ATTR_PARAM_VALUE, REPO_ATTR_PARAM_DESCRIPTION);
            loadParamListFromRepository(rep, stepId, xFormParams = new ArrayList<>(),
                    REPO_ATTR_XFORM_NAME, REPO_ATTR_XFORM_VALUE, REPO_ATTR_XFORM_DESCRIPTION);
        } catch (Exception e) {
            throw new KettleException("Unable to load step from repository", e);
        }
    }

    private void loadBasicAttributesFromRepository(Repository rep, ObjectId stepId)
            throws KettleException {
        url = rep.getStepAttributeString(stepId, XML_TAG_URL);
        requestMethod = rep.getStepAttributeString(stepId, XML_TAG_REQUEST_METHOD);
        requestType = rep.getStepAttributeString(stepId, XML_TAG_REQUEST_TYPE);
        useXForm = rep.getStepAttributeBoolean(stepId, XML_TAG_USE_XFORM);
        useRaw = rep.getStepAttributeBoolean(stepId, XML_TAG_USE_RAW);
        usePage = rep.getStepAttributeBoolean(stepId, XML_TAG_USE_PAGE);
        raw = rep.getStepAttributeString(stepId, XML_TAG_RAW);
        pageCondition = rep.getStepAttributeString(stepId, XML_TAG_PAGE_CONDITION);
        responseStatus = rep.getStepAttributeString(stepId, XML_TAG_RESPONSE_STATUS);
        responseHeader = rep.getStepAttributeString(stepId, XML_TAG_RESPONSE_HEADER);
        responseBody = rep.getStepAttributeString(stepId, XML_TAG_RESPONSE_BODY);
        requestCode = rep.getStepAttributeString(stepId, XML_TAG_REQUEST_CODE);
        responseCode = rep.getStepAttributeString(stepId, XML_TAG_RESPONSE_CODE);

        intervalFrom = rep.getStepAttributeInteger(stepId, XML_TAG_INTERVAL_FROM);
        intervalTo = rep.getStepAttributeInteger(stepId, XML_TAG_INTERVAL_TO);
        connectOutTime = (int) rep.getStepAttributeInteger(stepId, XML_TAG_CONNECT_TIMEOUT);
        readOutTime = (int) rep.getStepAttributeInteger(stepId, XML_TAG_READ_TIMEOUT);
        retryNum = (int) rep.getStepAttributeInteger(stepId, XML_TAG_RETRY_NUM);
        retryTime = rep.getStepAttributeInteger(stepId, XML_TAG_RETRY_TIME);
        startPageNo = rep.getStepAttributeInteger(stepId, XML_TAG_START_PAGE_NO);
    }

    private void loadParamListFromRepository(Repository rep, ObjectId stepId, List<Param> targetList,
                                             String nameAttr, String valueAttr, String descAttr)
            throws KettleException {
        int count = rep.countNrStepAttributes(stepId, nameAttr);

        for (int i = 0; i < count; i++) {
            targetList.add(new Param(
                    rep.getStepAttributeString(stepId, i, nameAttr),
                    rep.getStepAttributeString(stepId, i, valueAttr),
                    rep.getStepAttributeString(stepId, i, descAttr)
            ));
        }
    }

    @Override
    public Object clone() {
        ApiInputStepMeta clone = (ApiInputStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(clone, this);
            // 深拷贝列表
            clone.params = deepCopyParamList(this.params);
            clone.xFormParams = deepCopyParamList(this.xFormParams);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to clone ApiInputStepMeta", e);
        }
        return clone;
    }

    private List<Param> deepCopyParamList(List<Param> source) {
        if (source == null) {
            return new ArrayList<>();
        }

        List<Param> copy = new ArrayList<>(source.size());
        for (Param param : source) {
            copy.add(param.clone());
        }
        return copy;
    }

    @Override
    public void getFields(RowMetaInterface outputRowMeta, String name,
                          RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository,
                          IMetaStore metaStore) {
        outputRowMeta.addValueMeta(new ValueMetaInteger(responseStatus));
        outputRowMeta.addValueMeta(new ValueMetaString(responseHeader));
        outputRowMeta.addValueMeta(new ValueMetaString(responseBody));
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
                      StepMeta stepMeta, RowMetaInterface prev, String[] input,
                      String[] output, RowMetaInterface info, VariableSpace space,
                      Repository repository, IMetaStore metaStore) {

        // 检查URL配置
        if (isBlank(url)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    "URL配置为空！", stepMeta));
            return;
        }

        // 检查分页条件配置
        if (usePage && isBlank(pageCondition)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    "分页条件为空！", stepMeta));
            return;
        }

        // 检查间隔时间配置合理性
        if (intervalFrom != null && intervalTo != null && intervalFrom > intervalTo) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING,
                    "起始间隔时间大于结束间隔时间，将使用起始间隔时间作为固定间隔", stepMeta));
        }

        remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
                "校验通过！", stepMeta));
    }

    private boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    private Integer parseIntegerSafe(String value) {
        try {
            return value != null ? Integer.parseInt(value) : null;
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse integer value: {}", value, e);
            return null;
        }
    }

    private Long parseLongSafe(String value) {
        try {
            return value != null ? Long.parseLong(value) : null;
        } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse long value: {}", value, e);
            return null;
        }
    }

    // ==================== Getters and Setters ====================

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void setRequestMethod(String requestMethod) {
        this.requestMethod = requestMethod;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    public List<Param> getParams() {
        return params;
    }

    public void setParams(List<Param> params) {
        this.params = params;
    }

    public boolean isUseXForm() {
        return useXForm;
    }

    public void setUseXForm(boolean useXForm) {
        this.useXForm = useXForm;
    }

    public List<Param> getxFormParams() {
        return xFormParams;
    }

    public void setxFormParams(List<Param> xFormParams) {
        this.xFormParams = xFormParams;
    }

    public boolean isUseRaw() {
        return useRaw;
    }

    public void setUseRaw(boolean useRaw) {
        this.useRaw = useRaw;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    public boolean isUsePage() {
        return usePage;
    }

    public void setUsePage(boolean usePage) {
        this.usePage = usePage;
    }

    public String getPageCondition() {
        return pageCondition;
    }

    public void setPageCondition(String pageCondition) {
        this.pageCondition = pageCondition;
    }

    public String getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(String responseStatus) {
        this.responseStatus = responseStatus;
    }

    public String getResponseHeader() {
        return responseHeader;
    }

    public void setResponseHeader(String responseHeader) {
        this.responseHeader = responseHeader;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
    }

    public Long getIntervalFrom() {
        return intervalFrom;
    }

    public void setIntervalFrom(Long intervalFrom) {
        this.intervalFrom = intervalFrom;
    }

    public Long getIntervalTo() {
        return intervalTo;
    }

    public void setIntervalTo(Long intervalTo) {
        this.intervalTo = intervalTo;
    }

    public String getRequestCode() {
        return requestCode;
    }

    public void setRequestCode(String requestCode) {
        this.requestCode = requestCode;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(String responseCode) {
        this.responseCode = responseCode;
    }

    public Integer getConnectOutTime() {
        return connectOutTime;
    }

    public void setConnectOutTime(Integer connectOutTime) {
        this.connectOutTime = connectOutTime;
    }

    public Integer getReadOutTime() {
        return readOutTime;
    }

    public void setReadOutTime(Integer readOutTime) {
        this.readOutTime = readOutTime;
    }

    public Integer getRetryNum() {
        return retryNum;
    }

    public void setRetryNum(Integer retryNum) {
        this.retryNum = retryNum;
    }

    public Long getRetryTime() {
        return retryTime;
    }

    public void setRetryTime(Long retryTime) {
        this.retryTime = retryTime;
    }

    public Long getStartPageNo() {
        return startPageNo;
    }

    public void setStartPageNo(Long startPageNo) {
        this.startPageNo = startPageNo;
    }
}