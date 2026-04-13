package com.pufferfishscheduler.plugin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.InjectionSupported;
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
import org.w3c.dom.Node;

/**
 * RabbitMQ 输出：对齐 Spring publisher-confirm-type / publisher-returns / template.mandatory。
 */
@InjectionSupported(localizationPrefix = "RabbitMqProducerOutputMeta.Injection.", groups = {"CONFIGURATION_PROPERTIES"})
@Step(
        id = "RabbitMqProducerOutput",
        image = "rabbitmq-output.svg",
        name = "RabbitMQ输出",
        description = "向 RabbitMQ 交换机或队列发布消息。",
        categoryDescription = "输出")
public class RabbitMqProducerOutputMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String ADVANCED_CONFIG = "advancedConfig";

    public enum PublisherConfirmType {
        NONE,
        SIMPLE,
        CORRELATED
    }

    private String host = "";
    private String port = "5672";
    private String username = "";
    private String password = "";
    private String virtualHost = "/";

    /** 空串表示默认交换机（此时 routingKey 通常为队列名） */
    private String exchange = "";

    private String routingKey = "";

    private String messageField = "message";
    private String routingKeyField = "";

    private PublisherConfirmType publisherConfirmType = PublisherConfirmType.CORRELATED;
    private boolean publisherReturns = true;
    private boolean mandatory = true;

    private Map<String, String> config = new LinkedHashMap<>();

    @Override
    public void setDefault() {
        host = "";
        port = "5672";
        username = "";
        password = "";
        virtualHost = "/";
        exchange = "";
        routingKey = "";
        messageField = "message";
        routingKeyField = "";
        publisherConfirmType = PublisherConfirmType.CORRELATED;
        publisherReturns = true;
        mandatory = true;
        config = new LinkedHashMap<>();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) {
        readData(stepnode);
    }

    private void readData(Node stepnode) {
        host = XMLHandler.getTagValue(stepnode, "host");
        port = XMLHandler.getTagValue(stepnode, "port");
        username = XMLHandler.getTagValue(stepnode, "username");
        password = XMLHandler.getTagValue(stepnode, "password");
        virtualHost = XMLHandler.getTagValue(stepnode, "virtualHost");
        exchange = nvl(XMLHandler.getTagValue(stepnode, "exchange"), "");
        routingKey = nvl(XMLHandler.getTagValue(stepnode, "routingKey"), "");
        messageField = nvl(XMLHandler.getTagValue(stepnode, "messageField"), "message");
        routingKeyField = nvl(XMLHandler.getTagValue(stepnode, "routingKeyField"), "");
        String pct = XMLHandler.getTagValue(stepnode, "publisherConfirmType");
        try {
            publisherConfirmType =
                    pct == null || pct.isEmpty() ? PublisherConfirmType.CORRELATED : PublisherConfirmType.valueOf(pct);
        } catch (IllegalArgumentException e) {
            publisherConfirmType = PublisherConfirmType.NONE;
        }
        publisherReturns = !"N".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "publisherReturns"));
        mandatory = !"N".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "mandatory"));

        this.config = new LinkedHashMap<>();
        Node advancedNode = XMLHandler.getSubNode(stepnode, ADVANCED_CONFIG);
        if (advancedNode != null) {
            IntStream.range(0, advancedNode.getChildNodes().getLength())
                    .mapToObj(advancedNode.getChildNodes()::item)
                    .filter(n -> n.getNodeType() == Node.ELEMENT_NODE)
                    .forEach(n -> {
                        if ("option".equals(n.getNodeName()) && n.getAttributes() != null
                                && n.getAttributes().getNamedItem("property") != null
                                && n.getAttributes().getNamedItem("value") != null) {
                            this.config.put(
                                    n.getAttributes().getNamedItem("property").getTextContent(),
                                    n.getAttributes().getNamedItem("value").getTextContent());
                        } else {
                            this.config.put(n.getNodeName(), n.getTextContent());
                        }
                    });
        }
    }

    private static String nvl(String v, String d) {
        return v == null ? d : v;
    }

    @Override
    public String getXML() {
        StringBuilder xml = new StringBuilder();
        xml.append("    ").append(XMLHandler.addTagValue("host", host));
        xml.append("    ").append(XMLHandler.addTagValue("port", port));
        xml.append("    ").append(XMLHandler.addTagValue("username", username));
        xml.append("    ").append(XMLHandler.addTagValue("password", password));
        xml.append("    ").append(XMLHandler.addTagValue("virtualHost", virtualHost));
        xml.append("    ").append(XMLHandler.addTagValue("exchange", exchange));
        xml.append("    ").append(XMLHandler.addTagValue("routingKey", routingKey));
        xml.append("    ").append(XMLHandler.addTagValue("messageField", messageField));
        xml.append("    ").append(XMLHandler.addTagValue("routingKeyField", routingKeyField));
        xml.append("    ").append(XMLHandler.addTagValue("publisherConfirmType", publisherConfirmType.name()));
        xml.append("    ").append(XMLHandler.addTagValue("publisherReturns", publisherReturns));
        xml.append("    ").append(XMLHandler.addTagValue("mandatory", mandatory));
        xml.append("    ").append(XMLHandler.openTag(ADVANCED_CONFIG)).append(Const.CR);
        for (Map.Entry<String, String> entry : getConfig().entrySet()) {
            xml.append("        ")
                    .append(XMLHandler.addTagValue(
                            "option", "", true,
                            new String[] {"property", entry.getKey(), "value", entry.getValue()}));
        }
        xml.append("    ").append(XMLHandler.closeTag(ADVANCED_CONFIG)).append(Const.CR);
        return xml.toString();
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
            throws KettleException {
        host = rep.getStepAttributeString(stepId, "host");
        port = rep.getStepAttributeString(stepId, "port");
        username = rep.getStepAttributeString(stepId, "username");
        password = rep.getStepAttributeString(stepId, "password");
        virtualHost = rep.getStepAttributeString(stepId, "virtualHost");
        exchange = nvl(rep.getStepAttributeString(stepId, "exchange"), "");
        routingKey = nvl(rep.getStepAttributeString(stepId, "routingKey"), "");
        messageField = nvl(rep.getStepAttributeString(stepId, "messageField"), "message");
        routingKeyField = nvl(rep.getStepAttributeString(stepId, "routingKeyField"), "");
        String pct = rep.getStepAttributeString(stepId, "publisherConfirmType");
        try {
            publisherConfirmType =
                    pct == null || pct.isEmpty() ? PublisherConfirmType.CORRELATED : PublisherConfirmType.valueOf(pct);
        } catch (IllegalArgumentException e) {
            publisherConfirmType = PublisherConfirmType.NONE;
        }
        publisherReturns = rep.getStepAttributeBoolean(stepId, "publisherReturns");
        mandatory = rep.getStepAttributeBoolean(stepId, "mandatory");

        this.config = new LinkedHashMap<>();
        int n = (int) rep.getStepAttributeInteger(stepId, ADVANCED_CONFIG + "_COUNT");
        for (int i = 0; i < n; i++) {
            this.config.put(
                    rep.getStepAttributeString(stepId, i, ADVANCED_CONFIG + "_NAME"),
                    rep.getStepAttributeString(stepId, i, ADVANCED_CONFIG + "_VALUE"));
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId)
            throws KettleException {
        rep.saveStepAttribute(transformationId, stepId, "host", host);
        rep.saveStepAttribute(transformationId, stepId, "port", port);
        rep.saveStepAttribute(transformationId, stepId, "username", username);
        rep.saveStepAttribute(transformationId, stepId, "password", password);
        rep.saveStepAttribute(transformationId, stepId, "virtualHost", virtualHost);
        rep.saveStepAttribute(transformationId, stepId, "exchange", exchange);
        rep.saveStepAttribute(transformationId, stepId, "routingKey", routingKey);
        rep.saveStepAttribute(transformationId, stepId, "messageField", messageField);
        rep.saveStepAttribute(transformationId, stepId, "routingKeyField", routingKeyField);
        rep.saveStepAttribute(transformationId, stepId, "publisherConfirmType", publisherConfirmType.name());
        rep.saveStepAttribute(transformationId, stepId, "publisherReturns", publisherReturns);
        rep.saveStepAttribute(transformationId, stepId, "mandatory", mandatory);
        Map<String, String> cfg = getConfig();
        rep.saveStepAttribute(transformationId, stepId, ADVANCED_CONFIG + "_COUNT", cfg.size());
        int i = 0;
        for (Map.Entry<String, String> e : cfg.entrySet()) {
            rep.saveStepAttribute(transformationId, stepId, i, ADVANCED_CONFIG + "_NAME", e.getKey());
            rep.saveStepAttribute(transformationId, stepId, i, ADVANCED_CONFIG + "_VALUE", e.getValue());
            i++;
        }
    }

    @Override
    public Object clone() {
        RabbitMqProducerOutputMeta m = (RabbitMqProducerOutputMeta) super.clone();
        m.config = new LinkedHashMap<>(this.config);
        return m;
    }

    @Override
    public void getFields(
            RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
            Repository repository, IMetaStore metaStore) {
        // 输出步骤不新增字段
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
            Trans trans) {
        return new RabbitMqProducerOutput(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new RabbitMqProducerOutputData();
    }

    public Map<String, String> getConfig() {
        return config != null ? config : new LinkedHashMap<>();
    }

    public void setConfig(Map<String, String> config) {
        this.config = config != null ? new LinkedHashMap<>(config) : new LinkedHashMap<>();
    }

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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange == null ? "" : exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey == null ? "" : routingKey;
    }

    public String getMessageField() {
        return messageField;
    }

    public void setMessageField(String messageField) {
        this.messageField = messageField;
    }

    public String getRoutingKeyField() {
        return routingKeyField;
    }

    public void setRoutingKeyField(String routingKeyField) {
        this.routingKeyField = routingKeyField;
    }

    public PublisherConfirmType getPublisherConfirmType() {
        return publisherConfirmType;
    }

    public void setPublisherConfirmType(PublisherConfirmType publisherConfirmType) {
        this.publisherConfirmType = publisherConfirmType == null ? PublisherConfirmType.NONE : publisherConfirmType;
    }

    public boolean isPublisherReturns() {
        return publisherReturns;
    }

    public void setPublisherReturns(boolean publisherReturns) {
        this.publisherReturns = publisherReturns;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }
}
