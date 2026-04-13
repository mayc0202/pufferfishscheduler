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
 * Redis输出步骤元数据类
 * 用于配置Redis连接参数和写入参数
 */
@Step(
        id = "RedisOutput",
        name = "Redis输出",
        description = "将内容输入到redis中",
        image = "redis-output.svg",
        categoryDescription = "输出"
)
public class RedisOutputStepMeta extends BaseStepMeta implements StepMetaInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOutputStepMeta.class);
    private static final Class<?> PKG = RedisOutputStepMeta.class;

    // XML标签常量
    private static final String XML_TAG_HOST = "host";
    private static final String XML_TAG_PORT = "port";
    private static final String XML_TAG_PASSWORD = "password";
    private static final String XML_TAG_DB_NAME = "dbName";
    private static final String XML_TAG_KEY = "key";
    private static final String XML_TAG_VALUE = "value";

    // 默认值常量
    private static final String DEFAULT_HOST = "";
    private static final String DEFAULT_PORT = "6379";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_DB_NAME = "";
    private static final String DEFAULT_KEY = "";
    private static final String DEFAULT_VALUE = "";

    // 配置属性
    private String host;
    private String port;
    private String password;
    private String dbName;
    private String key;
    private String value;

    public RedisOutputStepMeta() {
        setDefault();
    }

    @Override
    public void setDefault() {
        this.host = DEFAULT_HOST;
        this.port = DEFAULT_PORT;
        this.password = DEFAULT_PASSWORD;
        this.dbName = DEFAULT_DB_NAME;
        this.key = DEFAULT_KEY;
        this.value = DEFAULT_VALUE;
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                                 int copyNr, TransMeta transMeta, Trans trans) {
        return new RedisOutputStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new RedisOutputStepData();
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder xml = new StringBuilder(512);

        appendXmlTag(xml, XML_TAG_HOST, host);
        appendXmlTag(xml, XML_TAG_PORT, port);
        appendXmlTag(xml, XML_TAG_PASSWORD, maskPassword(password));
        appendXmlTag(xml, XML_TAG_DB_NAME, dbName);
        appendXmlTag(xml, XML_TAG_KEY, key);
        appendXmlTag(xml, XML_TAG_VALUE, value);

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
            value = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, XML_TAG_VALUE));

            // 使用默认值处理空值
            applyDefaultIfNeeded();
        } catch (Exception e) {
            throw new KettleXMLException("Redis plugin unable to read step info from XML node", e);
        }
    }

    private void applyDefaultIfNeeded() {
        if (isBlank(host)) host = DEFAULT_HOST;
        if (isBlank(port)) port = DEFAULT_PORT;
        if (isBlank(key)) key = DEFAULT_KEY;
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
            rep.saveStepAttribute(transformationId, stepId, XML_TAG_VALUE, value);
        } catch (Exception e) {
            throw new KettleException(
                    BaseMessages.getString(PKG, "RedisOutputStepMeta.Exception.UnableToSaveStepInfoToRepository"),
                    e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore,
                        ObjectId stepId, List<DatabaseMeta> databases) throws KettleException {
        try {
            setDefault();

            host = rep.getStepAttributeString(stepId, XML_TAG_HOST);
            port = rep.getStepAttributeString(stepId, XML_TAG_PORT);
            password = rep.getStepAttributeString(stepId, XML_TAG_PASSWORD);
            dbName = rep.getStepAttributeString(stepId, XML_TAG_DB_NAME);
            key = rep.getStepAttributeString(stepId, XML_TAG_KEY);
            value = rep.getStepAttributeString(stepId, XML_TAG_VALUE);

            applyDefaultIfNeeded();
        } catch (Exception e) {
            throw new KettleException(
                    BaseMessages.getString(PKG, "RedisOutputStepMeta.Exception.ErrorReadingStepInfoFromRepository"),
                    e);
        }
    }

    @Override
    public Object clone() {
        RedisOutputStepMeta clone = (RedisOutputStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(clone, this);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to clone RedisOutputStepMeta", e);
        }
        return clone;
    }

    @Override
    public void getFields(RowMetaInterface outputRowMeta, String origin,
                          RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository,
                          IMetaStore metaStore) throws KettleStepException {
        // Redis输出步骤不添加额外输出字段，保持原有字段不变
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
                      StepMeta stepMeta, RowMetaInterface prev, String[] input,
                      String[] output, RowMetaInterface info, VariableSpace space,
                      Repository repository, IMetaStore metaStore) {

        // 检查主机配置
        if (isBlank(host)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    "Redis主机地址不能为空！", stepMeta));
            return;
        }

        // 检查端口配置
        if (!isValidPort(port)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    "Redis端口无效，请输入1-65535之间的端口号！", stepMeta));
            return;
        }

        // 检查Key配置
        if (isBlank(key)) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING,
                    "Redis Key未配置，将从输入流中获取Key字段！", stepMeta));
        }

        // 对于输出步骤，通常需要有输入流
        if (input == null || input.length == 0) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING,
                    "该步骤没有输入流，请确保这是您预期的配置！", stepMeta));
        }

        remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
                "校验通过！", stepMeta));
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}