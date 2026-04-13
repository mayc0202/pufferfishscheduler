package com.pufferfishscheduler.plugin;

import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.Injection;
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
 * Kafka 输出步骤元数据（直连 bootstrap；高级参数与数据源 properties 合并进 config）。
 */
@InjectionSupported(localizationPrefix = "KafkaProducerOutputMeta.Injection.", groups = {"CONFIGURATION_PROPERTIES"})
@Step(id = "KafkaProducerOutput", image = "kafka-output.svg", name = "Kafka输出", description = "向 Kafka 主题写入数据。", categoryDescription = "输出")
public class KafkaProducerOutputMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String CONNECTION_TYPE = "connectionType";
    public static final String DIRECT_BOOTSTRAP_SERVERS = "directBootstrapServers";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String CLIENT_ID = "clientId";
    public static final String TOPIC = "topic";
    public static final String KEY_FIELD = "keyField";
    public static final String MESSAGE_FIELD = "messageField";
    public static final String ADVANCED_CONFIG = "advancedConfig";

    @Injection(name = "DIRECT_BOOTSTRAP_SERVERS")
    private String directBootstrapServers;

    @Injection(name = "CLUSTER_NAME")
    private String clusterName;

    @Injection(name = "CLIENT_ID")
    private String clientId;

    @Injection(name = "TOPIC")
    private String topicVal;

    @Injection(name = "KEY_FIELD")
    private String keyField;

    @Injection(name = "MESSAGE_FIELD")
    private String messageField;

    @Injection(name = "NAMES", group = "CONFIGURATION_PROPERTIES")
    protected List<String> injectedConfigNames;

    @Injection(name = "VALUES", group = "CONFIGURATION_PROPERTIES")
    protected List<String> injectedConfigValues;

    @Injection(name = "CONNECTION_TYPE")
    private ConnectionType connectionType = ConnectionType.DIRECT;

    private Map<String, String> config = new LinkedHashMap<>();

    public enum ConnectionType {
        DIRECT,
        CLUSTER
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) {
        readData(stepnode);
    }

    private void readData(Node stepnode) {
        String ct = XMLHandler.getTagValue(stepnode, CONNECTION_TYPE);
        try {
            setConnectionType(ct == null || ct.isEmpty() ? ConnectionType.DIRECT : ConnectionType.valueOf(ct));
        } catch (IllegalArgumentException e) {
            setConnectionType(ConnectionType.DIRECT);
        }
        setDirectBootstrapServers(XMLHandler.getTagValue(stepnode, DIRECT_BOOTSTRAP_SERVERS));
        setClusterName(XMLHandler.getTagValue(stepnode, CLUSTER_NAME));
        setClientId(XMLHandler.getTagValue(stepnode, CLIENT_ID));
        setTopic(XMLHandler.getTagValue(stepnode, TOPIC));
        setKeyField(XMLHandler.getTagValue(stepnode, KEY_FIELD));
        setMessageField(XMLHandler.getTagValue(stepnode, MESSAGE_FIELD));
        this.config = new LinkedHashMap<>();
        org.w3c.dom.Node advancedNode = XMLHandler.getSubNode(stepnode, ADVANCED_CONFIG);
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

    @Override
    public void setDefault() {
        this.connectionType = ConnectionType.DIRECT;
        this.directBootstrapServers = "localhost:9092";
        this.clientId = "pufferfish-kafka-producer";
        this.topicVal = "";
        this.keyField = "";
        this.messageField = "message";
        this.config = new LinkedHashMap<>();
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
            throws KettleException {
        String ct = rep.getStepAttributeString(stepId, CONNECTION_TYPE);
        try {
            setConnectionType(ct == null || ct.isEmpty() ? ConnectionType.DIRECT : ConnectionType.valueOf(ct));
        } catch (IllegalArgumentException e) {
            setConnectionType(ConnectionType.DIRECT);
        }
        setDirectBootstrapServers(rep.getStepAttributeString(stepId, DIRECT_BOOTSTRAP_SERVERS));
        setClusterName(rep.getStepAttributeString(stepId, CLUSTER_NAME));
        setClientId(rep.getStepAttributeString(stepId, CLIENT_ID));
        setTopic(rep.getStepAttributeString(stepId, TOPIC));
        setKeyField(rep.getStepAttributeString(stepId, KEY_FIELD));
        setMessageField(rep.getStepAttributeString(stepId, MESSAGE_FIELD));
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
        rep.saveStepAttribute(transformationId, stepId, CONNECTION_TYPE, this.connectionType.name());
        rep.saveStepAttribute(transformationId, stepId, DIRECT_BOOTSTRAP_SERVERS, this.directBootstrapServers);
        rep.saveStepAttribute(transformationId, stepId, CLUSTER_NAME, this.clusterName);
        rep.saveStepAttribute(transformationId, stepId, CLIENT_ID, this.clientId);
        rep.saveStepAttribute(transformationId, stepId, TOPIC, this.topicVal);
        rep.saveStepAttribute(transformationId, stepId, KEY_FIELD, this.keyField);
        rep.saveStepAttribute(transformationId, stepId, MESSAGE_FIELD, this.messageField);
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
    public String getXML() {
        StringBuilder xml = new StringBuilder();
        xml.append("    ").append(XMLHandler.addTagValue(CONNECTION_TYPE, this.connectionType.name()));
        xml.append("    ").append(XMLHandler.addTagValue(DIRECT_BOOTSTRAP_SERVERS, this.directBootstrapServers));
        xml.append("    ").append(XMLHandler.addTagValue(CLUSTER_NAME, this.clusterName));
        xml.append("    ").append(XMLHandler.addTagValue(TOPIC, this.topicVal));
        xml.append("    ").append(XMLHandler.addTagValue(CLIENT_ID, this.clientId));
        xml.append("    ").append(XMLHandler.addTagValue(KEY_FIELD, this.keyField));
        xml.append("    ").append(XMLHandler.addTagValue(MESSAGE_FIELD, this.messageField));
        xml.append("    ").append(XMLHandler.openTag(ADVANCED_CONFIG)).append(Const.CR);
        for (Map.Entry<String, String> entry : getConfig().entrySet()) {
            xml.append("        ").append(XMLHandler.addTagValue(
                    "option", "", true,
                    new String[]{"property", entry.getKey(), "value", entry.getValue()}));
        }
        xml.append("    ").append(XMLHandler.closeTag(ADVANCED_CONFIG)).append(Const.CR);
        return xml.toString();
    }

    @Override
    public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
            VariableSpace space, Repository repository, IMetaStore metaStore) {
        // 输出步骤不产生新字段
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
            Trans trans) {
        return new KafkaProducerOutput(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new KafkaProducerOutputData();
    }

    public String getBootstrapServers() {
        return getDirectBootstrapServers() == null ? "" : getDirectBootstrapServers();
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTopic() {
        return this.topicVal;
    }

    public void setTopic(String topic) {
        this.topicVal = topic;
    }

    public String getKeyField() {
        return this.keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public String getMessageField() {
        return this.messageField;
    }

    public void setMessageField(String messageField) {
        this.messageField = messageField;
    }

    public ConnectionType getConnectionType() {
        return this.connectionType;
    }

    public void setConnectionType(ConnectionType connectionType) {
        this.connectionType = connectionType;
    }

    public String getDirectBootstrapServers() {
        return this.directBootstrapServers;
    }

    public void setDirectBootstrapServers(String directBootstrapServers) {
        this.directBootstrapServers = directBootstrapServers;
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Map<String, String> getConfig() {
        applyInjectedProperties();
        return this.config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config != null ? new LinkedHashMap<>(config) : new LinkedHashMap<>();
    }

    protected void applyInjectedProperties() {
        if (this.injectedConfigNames == null && this.injectedConfigValues == null) {
            return;
        }
        Preconditions.checkState(this.injectedConfigNames != null, "Options names were not injected");
        Preconditions.checkState(this.injectedConfigValues != null, "Options values were not injected");
        Preconditions.checkState(this.injectedConfigNames.size() == this.injectedConfigValues.size(),
                "Injected different number of options names and value");
        Stream<Integer> streamBoxed = IntStream.range(0, this.injectedConfigNames.size()).boxed();
        List<String> list = this.injectedConfigNames;
        Function<Integer, String> nameFn = list::get;
        List<String> list2 = this.injectedConfigValues;
        Function<Integer, String> valFn = list2::get;
        setConfig(streamBoxed.collect(Collectors.toMap(nameFn, valFn, (v1, v2) -> v1, LinkedHashMap::new)));
        this.injectedConfigNames = null;
        this.injectedConfigValues = null;
    }
}
