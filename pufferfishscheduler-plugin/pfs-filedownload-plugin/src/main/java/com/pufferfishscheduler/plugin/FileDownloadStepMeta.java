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
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
 * 文件下载步骤元数据类
 * 用于配置文件下载参数，支持GET/POST方式下载附件
 */
@Step(
        id = "FileDownload",
        name = "附件下载",
        description = "使用GET/POST方式，下载指定地址的附件。",
        image = "FileDownload.svg",
        categoryDescription = "转换"
)
public class FileDownloadStepMeta extends BaseStepMeta implements StepMetaInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileDownloadStepMeta.class);

    // XML标签常量
    private static final String XML_TAG_URL = "url";
    private static final String XML_TAG_REQUEST_METHOD = "requestMethod";
    private static final String XML_TAG_REQUEST_TYPE = "requestType";
    private static final String XML_TAG_USE_XFORM = "useXForm";
    private static final String XML_TAG_USE_RAW = "useRaw";
    private static final String XML_TAG_RAW = "raw";
    private static final String XML_TAG_FILE_PATH = "filePath";
    private static final String XML_TAG_RETRY_NUM = "retryNum";
    private static final String XML_TAG_RETRY_TIME = "retryTime";
    private static final String XML_TAG_FILE_NAME = "fileName";
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
    private static final String DEFAULT_FILE_PATH = "";
    private static final String DEFAULT_FILE_NAME = "";
    private static final int DEFAULT_RETRY_NUM = 0;
    private static final long DEFAULT_RETRY_TIME_MS = 10000L;
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
    private String filePath;
    private Integer retryNum;
    private Long retryTime;
    private String fileName;

    public FileDownloadStepMeta() {
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
        this.filePath = DEFAULT_FILE_PATH;
        this.retryNum = DEFAULT_RETRY_NUM;
        this.retryTime = DEFAULT_RETRY_TIME_MS;
        this.fileName = DEFAULT_FILE_NAME;
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                                 int copyNr, TransMeta transMeta, Trans trans) {
        return new FileDownloadStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new FileDownloadStepData();
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
        appendXmlTag(xml, XML_TAG_RAW, raw);
        appendXmlTag(xml, XML_TAG_FILE_PATH, filePath);
        appendXmlTag(xml, XML_TAG_RETRY_NUM, retryNum);
        appendXmlTag(xml, XML_TAG_RETRY_TIME, retryTime);
        appendXmlTag(xml, XML_TAG_FILE_NAME, fileName);

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
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RAW, raw);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_FILE_PATH, filePath);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RETRY_NUM, retryNum);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_RETRY_TIME, retryTime);
        rep.saveStepAttribute(transformationId, stepId, XML_TAG_FILE_NAME, fileName);
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
            throw new KettleXMLException("FileDownload plugin unable to read step info from XML node", e);
        }
    }

    private void loadBasicAttributesFromXml(Node stepNode) {
        url = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_URL));
        requestMethod = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_REQUEST_METHOD));
        requestType = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_REQUEST_TYPE));
        useXForm = YES_VALUE.equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_USE_XFORM)));
        useRaw = YES_VALUE.equals(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_USE_RAW)));
        raw = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_RAW));
        filePath = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_FILE_PATH));
        fileName = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_FILE_NAME));

        retryNum = parseIntegerSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_RETRY_NUM)));
        retryTime = parseLongSafe(XMLHandler.getNodeValue(
                XMLHandler.getSubNode(stepNode, XML_TAG_RETRY_TIME)));
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
        raw = rep.getStepAttributeString(stepId, XML_TAG_RAW);
        filePath = rep.getStepAttributeString(stepId, XML_TAG_FILE_PATH);
        fileName = rep.getStepAttributeString(stepId, XML_TAG_FILE_NAME);

        retryNum = (int) rep.getStepAttributeInteger(stepId, XML_TAG_RETRY_NUM);
        retryTime = rep.getStepAttributeInteger(stepId, XML_TAG_RETRY_TIME);
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
        FileDownloadStepMeta clone = (FileDownloadStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(clone, this);
            // 深拷贝列表
            clone.params = deepCopyParamList(this.params);
            clone.xFormParams = deepCopyParamList(this.xFormParams);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to clone FileDownloadStepMeta", e);
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
                          IMetaStore metaStore) throws KettleStepException {
        // 文件下载步骤不添加额外输出字段
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

        // 检查文件保存路径配置
        if (isBlank(filePath)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING,
                    "文件保存路径为空，将保存到Kettle工作目录！", stepMeta));
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

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
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

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}