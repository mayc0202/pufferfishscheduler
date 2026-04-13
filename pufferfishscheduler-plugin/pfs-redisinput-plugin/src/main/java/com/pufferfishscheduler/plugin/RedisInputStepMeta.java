package com.pufferfishscheduler.plugin;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
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
 * Redis输入步骤元数据类
 * 用于配置Redis连接参数和读取参数
 */
@Step(
        id = "RedisInput",
        name = "redis输入",
        description = "读取redis中的数据。",
        image = "RedisInput.svg",
        categoryDescription = "输入"
)
public class RedisInputStepMeta extends BaseStepMeta implements StepMetaInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisInputStepMeta.class);
    private static final Class<?> PKG = RedisInputStepMeta.class;

    // XML标签常量
    private static final String XML_TAG_HOST = "host";
    private static final String XML_TAG_PORT = "port";
    private static final String XML_TAG_PASSWORD = "password";
    private static final String XML_TAG_DB_NAME = "dbName";
    private static final String XML_TAG_KEY = "key";
    private static final String XML_TAG_FIELD_DELIMITER = "fieldDelimiter";

    // 默认值常量
    private static final String DEFAULT_HOST = "";
    private static final String DEFAULT_PORT = "6379";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_DB_NAME = "";
    private static final String DEFAULT_KEY = "";
    private static final String DEFAULT_FIELD_DELIMITER = ",";

    // 输出字段名常量
    private static final String OUTPUT_FIELD_KEY = "_redis_key";
    private static final String OUTPUT_FIELD_VALUE = "_redis_value";

    // 配置属性
    private String host;
    private String port;
    private String password;
    private String dbName;
    private String key;
    private String fieldDelimiter;

    public RedisInputStepMeta() {
        setDefault();
    }

    @Override
    public void setDefault() {
        this.host = DEFAULT_HOST;
        this.port = DEFAULT_PORT;
        this.password = DEFAULT_PASSWORD;
        this.dbName = DEFAULT_DB_NAME;
        this.key = DEFAULT_KEY;
        this.fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                                 int copyNr, TransMeta transMeta, Trans trans) {
        return new RedisInputStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new RedisInputStepData();
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder xml = new StringBuilder(512);

        appendXmlTag(xml, XML_TAG_HOST, host);
        appendXmlTag(xml, XML_TAG_PORT, port);
        appendXmlTag(xml, XML_TAG_PASSWORD, maskPassword(password));
        appendXmlTag(xml, XML_TAG_DB_NAME, dbName);
        appendXmlTag(xml, XML_TAG_KEY, key);
        appendXmlTag(xml, XML_TAG_FIELD_DELIMITER, fieldDelimiter);

        return xml.toString();
    }

    private void appendXmlTag(StringBuilder xml, String tag, String value) {
        xml.append("    ").append(XMLHandler.addTagValue(tag, value));
    }

    private String maskPassword(String password) {
        if (password == null || password.isEmpty()) {
            return password;
        }
        // 密码加密处理，实际应该使用Kettle的加密机制
        return password;
    }

    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore)
            throws KettleXMLException {
        try {
            setDefault();

            host = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_HOST));
            port = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_PORT));
            password = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_PASSWORD));
            dbName = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_DB_NAME));
            key = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_KEY));
            fieldDelimiter = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_FIELD_DELIMITER));

            // 使用默认值处理空值
            applyDefaultIfNeeded();
        } catch (Exception e) {
            throw new KettleXMLException("Redis plugin unable to read step info from XML node", e);
        }
    }

    private void applyDefaultIfNeeded() {
        if (isBlank(host)) host = DEFAULT_HOST;
        if (isBlank(port)) port = DEFAULT_PORT;
        if (isBlank(fieldDelimiter)) fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore,
                        ObjectId transformationId, ObjectId stepId) throws KettleException {
        try {
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_HOST, host);
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_PORT, port);
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_PASSWORD, password);
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_DB_NAME, dbName);
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_KEY, key);
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_FIELD_DELIMITER, fieldDelimiter);
        } catch (Exception e) {
            throw new KettleException(
                    BaseMessages.getString(PKG, "RedisInputStepMeta.Exception.UnableToSaveStepInfoToRepository"),
                    e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore,
                        ObjectId stepId, List<DatabaseMeta> databases) throws KettleException {
        try {
            host = rep.getStepAttributeString(stepId, XML_TAG_HOST);
            port = rep.getStepAttributeString(stepId, XML_TAG_PORT);
            password = rep.getStepAttributeString(stepId, XML_TAG_PASSWORD);
            dbName = rep.getStepAttributeString(stepId, XML_TAG_DB_NAME);
            key = rep.getStepAttributeString(stepId, XML_TAG_KEY);
            fieldDelimiter = rep.getStepAttributeString(stepId, XML_TAG_FIELD_DELIMITER);

            applyDefaultIfNeeded();
        } catch (Exception e) {
            throw new KettleException(
                    BaseMessages.getString(PKG, "RowsFromResultMeta.Exception.ErrorReadingStepInfoFromRepository"),
                    e);
        }
    }

    @Override
    public Object clone() {
        RedisInputStepMeta clone = (RedisInputStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(clone, this);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to clone RedisInputStepMeta", e);
        }
        return clone;
    }

    @Override
    public void getFields(RowMetaInterface outputRowMeta, String origin,
                          RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository,
                          IMetaStore metaStore) throws KettleStepException {
        outputRowMeta.addValueMeta(new ValueMetaString(OUTPUT_FIELD_KEY));
        outputRowMeta.addValueMeta(new ValueMetaString(OUTPUT_FIELD_VALUE));
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
                      StepMeta stepMeta, RowMetaInterface prev, String[] input,
                      String[] output, RowMetaInterface info, VariableSpace space,
                      Repository repository, IMetaStore metaStore) {

        // 检查主机配置
        if (isBlank(host)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "RedisInputStepMeta.CheckResult.HostEmpty"),
                    stepMeta));
            return;
        }

        // 检查端口配置
        if (!isValidPort(port)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "RedisInputStepMeta.CheckResult.InvalidPort"),
                    stepMeta));
            return;
        }

        // 检查是否有输入流（对于输入步骤，通常不需要输入）
        if (input != null && input.length > 0) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING,
                    BaseMessages.getString(PKG, "RowsFromResultMeta.CheckResult.StepExpectingNoReadingInfoFromOtherSteps"),
                    stepMeta));
        }

        remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "RowsFromResultMeta.CheckResult.NoInputReceivedError"),
                stepMeta));
    }

    private boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    private boolean isValidPort(String portStr) {
        if (isBlank(portStr)) {
            return false;
        }
        try {
            int port = Integer.parseInt(portStr.trim());
            return port > 0 && port <= 65535;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 判断是否为集群模式
     */
    public boolean isClusterMode() {
        return host != null && host.contains(",");
    }

    /**
     * 获取主机列表
     */
    public String[] getHosts() {
        if (isBlank(host)) {
            return new String[0];
        }
        return host.split(",");
    }

    // ==================== Getters and Setters ====================

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }
}