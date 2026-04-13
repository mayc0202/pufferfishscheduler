package com.pufferfishscheduler.plugin;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
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
 * RabbitMQ 队列订阅元数据：连接信息来自数据源，监听与重试等由组件页配置。
 */
@Step(
        id = "RabbitMqConsumerInput",
        image = "rabbitmq-input.svg",
        name = "RabbitMQ输入",
        description = "订阅 RabbitMQ 队列消息。",
        categoryDescription = "输入")
public class RabbitMqConsumerInputMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String ADVANCED_CONFIG = "advancedConfig";

    private String host = "";
    private String port = "5672";
    private String username = "";
    private String password = "";
    private String virtualHost = "/";

    private String queue = "";

    /** false = 手动确认（ack），对齐 Spring listener.simple.acknowledge-mode: manual */
    private boolean autoAck = false;

    private int prefetch = 1;
    private int concurrency = 1;
    private int maxConcurrency = 1;

    private boolean defaultRequeueRejected = true;

    private boolean retryEnabled = true;
    private long retryInitialIntervalMs = 1000L;
    private int retryMaxAttempts = 3;
    private long retryMaxIntervalMs = 10000L;
    private double retryMultiplier = 1.0D;

    /** 0 表示不限制，直到转换停止 */
    private long maxMessages = 0L;

    private Map<String, String> config = new LinkedHashMap<>();

    private RabbitMqConsumerField messageField =
            new RabbitMqConsumerField(RabbitMqConsumerField.Name.MESSAGE, "message", RabbitMqConsumerField.Type.String);
    private RabbitMqConsumerField routingKeyField =
            new RabbitMqConsumerField(RabbitMqConsumerField.Name.ROUTING_KEY, "routingKey", RabbitMqConsumerField.Type.String);
    private RabbitMqConsumerField messageIdField =
            new RabbitMqConsumerField(RabbitMqConsumerField.Name.MESSAGE_ID, "messageId", RabbitMqConsumerField.Type.String);
    private RabbitMqConsumerField deliveryTagField =
            new RabbitMqConsumerField(RabbitMqConsumerField.Name.DELIVERY_TAG, "deliveryTag", RabbitMqConsumerField.Type.String);
    private RabbitMqConsumerField exchangeField =
            new RabbitMqConsumerField(RabbitMqConsumerField.Name.EXCHANGE, "exchange", RabbitMqConsumerField.Type.String);
    private RabbitMqConsumerField timestampField =
            new RabbitMqConsumerField(RabbitMqConsumerField.Name.TIMESTAMP, "timestamp", RabbitMqConsumerField.Type.Integer);

    @Override
    public void setDefault() {
        host = "";
        port = "5672";
        username = "";
        password = "";
        virtualHost = "/";
        queue = "";
        autoAck = false;
        prefetch = 1;
        concurrency = 1;
        maxConcurrency = 1;
        defaultRequeueRejected = true;
        retryEnabled = true;
        retryInitialIntervalMs = 1000L;
        retryMaxAttempts = 3;
        retryMaxIntervalMs = 10000L;
        retryMultiplier = 1.0D;
        maxMessages = 0L;
        config = new LinkedHashMap<>();
        messageField = new RabbitMqConsumerField(
                RabbitMqConsumerField.Name.MESSAGE, "message", RabbitMqConsumerField.Type.String);
        routingKeyField = new RabbitMqConsumerField(
                RabbitMqConsumerField.Name.ROUTING_KEY, "routingKey", RabbitMqConsumerField.Type.String);
        messageIdField = new RabbitMqConsumerField(
                RabbitMqConsumerField.Name.MESSAGE_ID, "messageId", RabbitMqConsumerField.Type.String);
        deliveryTagField = new RabbitMqConsumerField(
                RabbitMqConsumerField.Name.DELIVERY_TAG, "deliveryTag", RabbitMqConsumerField.Type.String);
        exchangeField = new RabbitMqConsumerField(
                RabbitMqConsumerField.Name.EXCHANGE, "exchange", RabbitMqConsumerField.Type.String);
        timestampField = new RabbitMqConsumerField(
                RabbitMqConsumerField.Name.TIMESTAMP, "timestamp", RabbitMqConsumerField.Type.Integer);
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        readData(stepnode);
    }

    private void readData(Node stepnode) {
        host = XMLHandler.getTagValue(stepnode, "host");
        port = XMLHandler.getTagValue(stepnode, "port");
        username = XMLHandler.getTagValue(stepnode, "username");
        password = XMLHandler.getTagValue(stepnode, "password");
        virtualHost = XMLHandler.getTagValue(stepnode, "virtualHost");
        queue = XMLHandler.getTagValue(stepnode, "queue");
        autoAck = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "autoAck"));
        prefetch = parseInt(XMLHandler.getTagValue(stepnode, "prefetch"), 1);
        concurrency = parseInt(XMLHandler.getTagValue(stepnode, "concurrency"), 1);
        maxConcurrency = parseInt(XMLHandler.getTagValue(stepnode, "maxConcurrency"), 1);
        defaultRequeueRejected = !"N".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "defaultRequeueRejected"));
        retryEnabled = !"N".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "retryEnabled"));
        retryInitialIntervalMs = parseLong(XMLHandler.getTagValue(stepnode, "retryInitialIntervalMs"), 1000L);
        retryMaxAttempts = parseInt(XMLHandler.getTagValue(stepnode, "retryMaxAttempts"), 3);
        retryMaxIntervalMs = parseLong(XMLHandler.getTagValue(stepnode, "retryMaxIntervalMs"), 10000L);
        retryMultiplier = parseDouble(XMLHandler.getTagValue(stepnode, "retryMultiplier"), 1.0D);
        maxMessages = parseLong(XMLHandler.getTagValue(stepnode, "maxMessages"), 0L);

        readField(stepnode, "messageField", messageField);
        readField(stepnode, "routingKeyField", routingKeyField);
        readField(stepnode, "messageIdField", messageIdField);
        readField(stepnode, "deliveryTagField", deliveryTagField);
        readField(stepnode, "exchangeField", exchangeField);
        readField(stepnode, "timestampField", timestampField);

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

    private static void readField(Node stepnode, String tag, RabbitMqConsumerField target) {
        Node n = XMLHandler.getSubNode(stepnode, tag);
        if (n == null) {
            return;
        }
        String out = XMLHandler.getTagValue(n, "outputName");
        String type = XMLHandler.getTagValue(n, "outputType");
        if (!Strings.isNullOrEmpty(out)) {
            target.setOutputName(out);
        }
        if (!Strings.isNullOrEmpty(type)) {
            try {
                target.setOutputType(RabbitMqConsumerField.Type.valueOf(type));
            } catch (IllegalArgumentException ignored) {
                target.setOutputType(RabbitMqConsumerField.Type.String);
            }
        }
    }

    private static int parseInt(String v, int def) {
        if (Strings.isNullOrEmpty(v)) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long parseLong(String v, long def) {
        if (Strings.isNullOrEmpty(v)) {
            return def;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double parseDouble(String v, double def) {
        if (Strings.isNullOrEmpty(v)) {
            return def;
        }
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    @Override
    public String getXML() {
        StringBuilder xml = new StringBuilder();
        xml.append("    ").append(XMLHandler.addTagValue("host", host));
        xml.append("    ").append(XMLHandler.addTagValue("port", port));
        xml.append("    ").append(XMLHandler.addTagValue("username", username));
        xml.append("    ").append(XMLHandler.addTagValue("password", password));
        xml.append("    ").append(XMLHandler.addTagValue("virtualHost", virtualHost));
        xml.append("    ").append(XMLHandler.addTagValue("queue", queue));
        xml.append("    ").append(XMLHandler.addTagValue("autoAck", autoAck));
        xml.append("    ").append(XMLHandler.addTagValue("prefetch", prefetch));
        xml.append("    ").append(XMLHandler.addTagValue("concurrency", concurrency));
        xml.append("    ").append(XMLHandler.addTagValue("maxConcurrency", maxConcurrency));
        xml.append("    ").append(XMLHandler.addTagValue("defaultRequeueRejected", defaultRequeueRejected));
        xml.append("    ").append(XMLHandler.addTagValue("retryEnabled", retryEnabled));
        xml.append("    ").append(XMLHandler.addTagValue("retryInitialIntervalMs", retryInitialIntervalMs));
        xml.append("    ").append(XMLHandler.addTagValue("retryMaxAttempts", retryMaxAttempts));
        xml.append("    ").append(XMLHandler.addTagValue("retryMaxIntervalMs", retryMaxIntervalMs));
        xml.append("    ").append(XMLHandler.addTagValue("retryMultiplier", retryMultiplier));
        xml.append("    ").append(XMLHandler.addTagValue("maxMessages", maxMessages));
        appendFieldXml(xml, "messageField", messageField);
        appendFieldXml(xml, "routingKeyField", routingKeyField);
        appendFieldXml(xml, "messageIdField", messageIdField);
        appendFieldXml(xml, "deliveryTagField", deliveryTagField);
        appendFieldXml(xml, "exchangeField", exchangeField);
        appendFieldXml(xml, "timestampField", timestampField);
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

    private static void appendFieldXml(StringBuilder xml, String tag, RabbitMqConsumerField f) {
        xml.append("    ").append(XMLHandler.openTag(tag)).append(Const.CR);
        xml.append("      ").append(XMLHandler.addTagValue("outputName", f.getOutputName()));
        xml.append("      ").append(XMLHandler.addTagValue("outputType", f.getOutputType().name()));
        xml.append("    ").append(XMLHandler.closeTag(tag)).append(Const.CR);
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
            throws KettleException {
        host = rep.getStepAttributeString(stepId, "host");
        port = rep.getStepAttributeString(stepId, "port");
        username = rep.getStepAttributeString(stepId, "username");
        password = rep.getStepAttributeString(stepId, "password");
        virtualHost = rep.getStepAttributeString(stepId, "virtualHost");
        queue = rep.getStepAttributeString(stepId, "queue");
        autoAck = rep.getStepAttributeBoolean(stepId, "autoAck");
        prefetch = (int) rep.getStepAttributeInteger(stepId, "prefetch");
        concurrency = (int) rep.getStepAttributeInteger(stepId, "concurrency");
        maxConcurrency = (int) rep.getStepAttributeInteger(stepId, "maxConcurrency");
        defaultRequeueRejected = rep.getStepAttributeBoolean(stepId, "defaultRequeueRejected");
        retryEnabled = rep.getStepAttributeBoolean(stepId, "retryEnabled");
        retryInitialIntervalMs = parseLong(rep.getStepAttributeString(stepId, "retryInitialIntervalMs"), 1000L);
        retryMaxAttempts = parseInt(rep.getStepAttributeString(stepId, "retryMaxAttempts"), 3);
        retryMaxIntervalMs = parseLong(rep.getStepAttributeString(stepId, "retryMaxIntervalMs"), 10000L);
        retryMultiplier = parseDouble(rep.getStepAttributeString(stepId, "retryMultiplier"), 1.0D);
        maxMessages = parseLong(rep.getStepAttributeString(stepId, "maxMessages"), 0L);

        readFieldRep(rep, stepId, "messageField", messageField);
        readFieldRep(rep, stepId, "routingKeyField", routingKeyField);
        readFieldRep(rep, stepId, "messageIdField", messageIdField);
        readFieldRep(rep, stepId, "deliveryTagField", deliveryTagField);
        readFieldRep(rep, stepId, "exchangeField", exchangeField);
        readFieldRep(rep, stepId, "timestampField", timestampField);

        this.config = new LinkedHashMap<>();
        int n = (int) rep.getStepAttributeInteger(stepId, ADVANCED_CONFIG + "_COUNT");
        for (int i = 0; i < n; i++) {
            this.config.put(
                    rep.getStepAttributeString(stepId, i, ADVANCED_CONFIG + "_NAME"),
                    rep.getStepAttributeString(stepId, i, ADVANCED_CONFIG + "_VALUE"));
        }
    }

    private static void readFieldRep(Repository rep, ObjectId stepId, String prefix, RabbitMqConsumerField f)
            throws KettleException {
        String out = rep.getStepAttributeString(stepId, prefix + "_outputName");
        String type = rep.getStepAttributeString(stepId, prefix + "_outputType");
        if (!Strings.isNullOrEmpty(out)) {
            f.setOutputName(out);
        }
        if (!Strings.isNullOrEmpty(type)) {
            try {
                f.setOutputType(RabbitMqConsumerField.Type.valueOf(type));
            } catch (IllegalArgumentException ignored) {
                f.setOutputType(RabbitMqConsumerField.Type.String);
            }
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
        rep.saveStepAttribute(transformationId, stepId, "queue", queue);
        rep.saveStepAttribute(transformationId, stepId, "autoAck", autoAck);
        rep.saveStepAttribute(transformationId, stepId, "prefetch", prefetch);
        rep.saveStepAttribute(transformationId, stepId, "concurrency", concurrency);
        rep.saveStepAttribute(transformationId, stepId, "maxConcurrency", maxConcurrency);
        rep.saveStepAttribute(transformationId, stepId, "defaultRequeueRejected", defaultRequeueRejected);
        rep.saveStepAttribute(transformationId, stepId, "retryEnabled", retryEnabled);
        rep.saveStepAttribute(transformationId, stepId, "retryInitialIntervalMs", String.valueOf(retryInitialIntervalMs));
        rep.saveStepAttribute(transformationId, stepId, "retryMaxAttempts", String.valueOf(retryMaxAttempts));
        rep.saveStepAttribute(transformationId, stepId, "retryMaxIntervalMs", String.valueOf(retryMaxIntervalMs));
        rep.saveStepAttribute(transformationId, stepId, "retryMultiplier", String.valueOf(retryMultiplier));
        rep.saveStepAttribute(transformationId, stepId, "maxMessages", String.valueOf(maxMessages));

        saveFieldRep(rep, transformationId, stepId, "messageField", messageField);
        saveFieldRep(rep, transformationId, stepId, "routingKeyField", routingKeyField);
        saveFieldRep(rep, transformationId, stepId, "messageIdField", messageIdField);
        saveFieldRep(rep, transformationId, stepId, "deliveryTagField", deliveryTagField);
        saveFieldRep(rep, transformationId, stepId, "exchangeField", exchangeField);
        saveFieldRep(rep, transformationId, stepId, "timestampField", timestampField);

        Map<String, String> cfg = getConfig();
        rep.saveStepAttribute(transformationId, stepId, ADVANCED_CONFIG + "_COUNT", cfg.size());
        int i = 0;
        for (Map.Entry<String, String> e : cfg.entrySet()) {
            rep.saveStepAttribute(transformationId, stepId, i, ADVANCED_CONFIG + "_NAME", e.getKey());
            rep.saveStepAttribute(transformationId, stepId, i, ADVANCED_CONFIG + "_VALUE", e.getValue());
            i++;
        }
    }

    private static void saveFieldRep(
            Repository rep, ObjectId transformationId, ObjectId stepId, String prefix, RabbitMqConsumerField f)
            throws KettleException {
        rep.saveStepAttribute(transformationId, stepId, prefix + "_outputName", f.getOutputName());
        rep.saveStepAttribute(transformationId, stepId, prefix + "_outputType", f.getOutputType().name());
    }

    @Override
    public Object clone() {
        RabbitMqConsumerInputMeta m = (RabbitMqConsumerInputMeta) super.clone();
        m.config = new LinkedHashMap<>(this.config);
        m.messageField = cloneField(messageField);
        m.routingKeyField = cloneField(routingKeyField);
        m.messageIdField = cloneField(messageIdField);
        m.deliveryTagField = cloneField(deliveryTagField);
        m.exchangeField = cloneField(exchangeField);
        m.timestampField = cloneField(timestampField);
        return m;
    }

    private static RabbitMqConsumerField cloneField(RabbitMqConsumerField f) {
        return new RabbitMqConsumerField(f.getRabbitName(), f.getOutputName(), f.getOutputType());
    }

    @Override
    public void getFields(
            RowMetaInterface row,
            String origin,
            RowMetaInterface[] info,
            StepMeta nextStep,
            VariableSpace space,
            Repository repository,
            IMetaStore metaStore)
            throws KettleStepException {
        for (RabbitMqConsumerField f : getFieldDefinitions()) {
            putField(row, origin, space, f);
        }
    }

    private void putField(RowMetaInterface row, String origin, VariableSpace space, RabbitMqConsumerField f)
            throws KettleStepException {
        if (f == null || Utils.isEmpty(f.getOutputName())) {
            return;
        }
        try {
            String name = space.environmentSubstitute(f.getOutputName());
            ValueMetaInterface v =
                    ValueMetaFactory.createValueMeta(name, f.getOutputType().getValueMetaInterfaceType());
            v.setOrigin(origin);
            row.addValueMeta(v);
        } catch (Exception e) {
            throw new KettleStepException("无法创建 RabbitMQ 输出列: " + f.getOutputName(), e);
        }
    }

    public List<RabbitMqConsumerField> getFieldDefinitions() {
        List<RabbitMqConsumerField> list = new ArrayList<>();
        list.add(messageField);
        list.add(routingKeyField);
        list.add(messageIdField);
        list.add(deliveryTagField);
        list.add(exchangeField);
        list.add(timestampField);
        return list;
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans) {
        return new RabbitMqConsumerInput(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new RabbitMqConsumerInputData();
    }

    public Map<String, String> getConfig() {
        return config != null ? config : new LinkedHashMap<>();
    }

    public void setConfig(Map<String, String> config) {
        this.config = config != null ? new LinkedHashMap<>(config) : new LinkedHashMap<>();
    }

    /** 对齐 Spring listener.simple：实际消费者数量取 concurrency 与 maxConcurrency 的合理区间。 */
    public int effectiveConsumerCount() {
        int c = Math.max(1, concurrency);
        int m = Math.max(1, maxConcurrency);
        return Math.min(c, m);
    }

    // --- getters / setters ---

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

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public int getPrefetch() {
        return prefetch;
    }

    public void setPrefetch(int prefetch) {
        this.prefetch = prefetch;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public boolean isDefaultRequeueRejected() {
        return defaultRequeueRejected;
    }

    public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
        this.defaultRequeueRejected = defaultRequeueRejected;
    }

    public boolean isRetryEnabled() {
        return retryEnabled;
    }

    public void setRetryEnabled(boolean retryEnabled) {
        this.retryEnabled = retryEnabled;
    }

    public long getRetryInitialIntervalMs() {
        return retryInitialIntervalMs;
    }

    public void setRetryInitialIntervalMs(long retryInitialIntervalMs) {
        this.retryInitialIntervalMs = retryInitialIntervalMs;
    }

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public void setRetryMaxAttempts(int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    public long getRetryMaxIntervalMs() {
        return retryMaxIntervalMs;
    }

    public void setRetryMaxIntervalMs(long retryMaxIntervalMs) {
        this.retryMaxIntervalMs = retryMaxIntervalMs;
    }

    public double getRetryMultiplier() {
        return retryMultiplier;
    }

    public void setRetryMultiplier(double retryMultiplier) {
        this.retryMultiplier = retryMultiplier;
    }

    public long getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(long maxMessages) {
        this.maxMessages = maxMessages;
    }

    public RabbitMqConsumerField getMessageField() {
        return messageField;
    }

    public void setMessageField(RabbitMqConsumerField messageField) {
        this.messageField = messageField;
    }

    public RabbitMqConsumerField getRoutingKeyField() {
        return routingKeyField;
    }

    public void setRoutingKeyField(RabbitMqConsumerField routingKeyField) {
        this.routingKeyField = routingKeyField;
    }

    public RabbitMqConsumerField getMessageIdField() {
        return messageIdField;
    }

    public void setMessageIdField(RabbitMqConsumerField messageIdField) {
        this.messageIdField = messageIdField;
    }

    public RabbitMqConsumerField getDeliveryTagField() {
        return deliveryTagField;
    }

    public void setDeliveryTagField(RabbitMqConsumerField deliveryTagField) {
        this.deliveryTagField = deliveryTagField;
    }

    public RabbitMqConsumerField getExchangeField() {
        return exchangeField;
    }

    public void setExchangeField(RabbitMqConsumerField exchangeField) {
        this.exchangeField = exchangeField;
    }

    public RabbitMqConsumerField getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(RabbitMqConsumerField timestampField) {
        this.timestampField = timestampField;
    }
}
