package com.pufferfishscheduler.plugin;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Kafka 流式输入元数据（直连集群 + 高级参数；子转换由 {@link BaseStreamStepMeta} 承载）。
 */
@InjectionSupported(localizationPrefix = "KafkaConsumerInputMeta.Injection.", groups = {"CONFIGURATION_PROPERTIES"})
@Step(id = "KafkaConsumerInput", image = "kafka-input.svg", name = KafkaConsumerInputDialog.L_DIALOGTITLE, description = "消费Kafka中消息。", categoryDescription = "输入")
public class KafkaConsumerInputMeta extends BaseStreamStepMeta implements StepMetaInterface {
    public static final String CLUSTER_NAME = "clusterName";
    public static final String TOPIC = "topic";
    public static final String CONSUMER_GROUP = "consumerGroup";
    public static final String TRANSFORMATION_PATH = "transformationPath";
    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_DURATION = "batchDuration";
    public static final String PREFETCH_MESSAGE_COUNT = "prefetchMessageCount";
    public static final String CONNECTION_TYPE = "connectionType";
    public static final String DIRECT_BOOTSTRAP_SERVERS = "directBootstrapServers";
    public static final String ADVANCED_CONFIG = "advancedConfig";
    public static final String CONFIG_OPTION = "option";
    public static final String OPTION_PROPERTY = "property";
    public static final String OPTION_VALUE = "value";
    public static final String TOPIC_FIELD_NAME = "topic";
    public static final String OFFSET_FIELD_NAME = "offset";
    public static final String PARTITION_FIELD_NAME = "partition";
    public static final String TIMESTAMP_FIELD_NAME = "timestamp";
    public static final String OUTPUT_FIELD_TAG_NAME = "OutputField";
    public static final String KAFKA_NAME_ATTRIBUTE = "kafkaName";
    public static final String TYPE_ATTRIBUTE = "type";
    public static final String AUTO_COMMIT = "AUTO_COMMIT";
    public static final String PARTITION = "partition";
    private static final Class<?> PKG = KafkaConsumerInput.class;

    /** 业务类型（与 KettleFlowRepository 等扩展字段兼容，可选） */
    private String bizType;
    /** 业务对象 ID（可选） */
    private String bizObjectId;

    @Injection(name = "DIRECT_BOOTSTRAP_SERVERS")
    private String directBootstrapServers;

    @Injection(name = "CLUSTER_NAME")
    private String clusterName;

    @Injection(name = "CONSUMER_GROUP")
    private String consumerGroup;
    private String partition;

    @Injection(name = "NAMES", group = "CONFIGURATION_PROPERTIES")
    protected List<String> injectedConfigNames;

    @Injection(name = "VALUES", group = "CONFIGURATION_PROPERTIES")
    protected List<String> injectedConfigValues;

    @Injection(name = "CONNECTION_TYPE")
    private ConnectionType connectionType = ConnectionType.DIRECT;

    @Injection(name = "TOPICS")
    private List<String> topics = new ArrayList();

    @Injection(name = AUTO_COMMIT)
    private boolean autoCommit = true;
    private Map<String, String> config = new LinkedHashMap();
    private KafkaFactory kafkaFactory = KafkaFactory.defaultFactory();

    @InjectionDeep(prefix = "KEY")
    private KafkaConsumerField keyField = new KafkaConsumerField(KafkaConsumerField.Name.KEY, "key");

    @InjectionDeep(prefix = "MESSAGE")
    private KafkaConsumerField messageField = new KafkaConsumerField(KafkaConsumerField.Name.MESSAGE, "message");
    private KafkaConsumerField topicField = new KafkaConsumerField(KafkaConsumerField.Name.TOPIC, "topic");
    private KafkaConsumerField partitionField = new KafkaConsumerField(KafkaConsumerField.Name.PARTITION, "partition", KafkaConsumerField.Type.Integer);
    private KafkaConsumerField offsetField = new KafkaConsumerField(KafkaConsumerField.Name.OFFSET, OFFSET_FIELD_NAME, KafkaConsumerField.Type.Integer);
    private KafkaConsumerField timestampField = new KafkaConsumerField(KafkaConsumerField.Name.TIMESTAMP, TIMESTAMP_FIELD_NAME, KafkaConsumerField.Type.Integer);

    public enum ConnectionType {
        DIRECT,
        CLUSTER
    }

    public KafkaConsumerInputMeta() {
        setSpecificationMethod(ObjectLocationSpecificationMethod.FILENAME);
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) {
        readData(stepnode);
    }

    private void readData(Node stepnode) {
        setClusterName(XMLHandler.getTagValue(stepnode, "clusterName"));
        List<Node> topicsNode = XMLHandler.getNodes(stepnode, "topic");
        topicsNode.forEach(node -> {
            String displayName = XMLHandler.getNodeValue(node);
            addTopic(displayName);
        });
        setConsumerGroup(XMLHandler.getTagValue(stepnode, CONSUMER_GROUP));
        setTransformationPath(XMLHandler.getTagValue(stepnode, TRANSFORMATION_PATH));
        String subStepTag = XMLHandler.getTagValue(stepnode, "SUB_STEP");
        if (!StringUtil.isEmpty(subStepTag)) {
            setSubStep(subStepTag);
        }
        setBizType(XMLHandler.getTagValue(stepnode, "bizType"));
        setBizObjectId(XMLHandler.getTagValue(stepnode, "bizObjectId"));
        setFileName(XMLHandler.getTagValue(stepnode, TRANSFORMATION_PATH));
        setBatchSize((String) Optional.ofNullable(XMLHandler.getTagValue(stepnode, BATCH_SIZE)).orElse(""));
        setBatchDuration((String) Optional.ofNullable(XMLHandler.getTagValue(stepnode, BATCH_DURATION)).orElse(""));
        String parallelism = XMLHandler.getTagValue(stepnode, "PARALLELISM");
        setParallelism(Strings.isNullOrEmpty(parallelism) ? "1" : parallelism);
        String prefetchCount = XMLHandler.getTagValue(stepnode, PREFETCH_MESSAGE_COUNT);
        setPrefetchCount(Strings.isNullOrEmpty(prefetchCount) ? PREFETCH_DEFAULT : prefetchCount);
        String connectionTypeTag = XMLHandler.getTagValue(stepnode, "connectionType");
        try {
            setConnectionType(Strings.isNullOrEmpty(connectionTypeTag) ? ConnectionType.DIRECT : ConnectionType.valueOf(connectionTypeTag));
        } catch (IllegalArgumentException ex) {
            setConnectionType(ConnectionType.DIRECT);
        }
        setDirectBootstrapServers(XMLHandler.getTagValue(stepnode, "directBootstrapServers"));
        String autoCommitValue = XMLHandler.getTagValue(stepnode, AUTO_COMMIT);
        setAutoCommit("Y".equals(autoCommitValue) || Strings.isNullOrEmpty(autoCommitValue));
        List<Node> ofNode = XMLHandler.getNodes(stepnode, OUTPUT_FIELD_TAG_NAME);
        setPartition(XMLHandler.getTagValue(stepnode, "partition"));
        ofNode.forEach(node2 -> {
            String displayName = XMLHandler.getNodeValue(node2);
            String kafkaName = XMLHandler.getTagAttribute(node2, KAFKA_NAME_ATTRIBUTE);
            String type = XMLHandler.getTagAttribute(node2, "type");
            KafkaConsumerField field = new KafkaConsumerField(KafkaConsumerField.Name.valueOf(kafkaName.toUpperCase()), displayName, KafkaConsumerField.Type.valueOf(type));
            setField(field);
        });
        this.config = new LinkedHashMap();
        Optional.ofNullable(XMLHandler.getSubNode(stepnode, "advancedConfig")).map((v0) -> {
            return v0.getChildNodes();
        }).ifPresent(nodes -> {
            IntStream intStreamRange = IntStream.range(0, nodes.getLength());
            nodes.getClass();
            intStreamRange.mapToObj(nodes::item).filter(node3 -> {
                return node3.getNodeType() == 1;
            }).forEach(node4 -> {
                if ("option".equals(node4.getNodeName())) {
                    this.config.put(node4.getAttributes().getNamedItem("property").getTextContent(), node4.getAttributes().getNamedItem("value").getTextContent());
                } else {
                    this.config.put(node4.getNodeName(), node4.getTextContent());
                }
            });
        });
    }

    protected void setField(KafkaConsumerField field) {
        field.getKafkaName().setFieldOnMeta(this, field);
    }

    public void setDefault() {
        this.batchSize = "1000";
        this.batchDuration = "1000";
        this.parallelism = "1";
        this.prefetchCount = PREFETCH_DEFAULT;
    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId objectId, List<DatabaseMeta> databases) throws KettleException {
        setClusterName(rep.getStepAttributeString(objectId, "clusterName"));
        int topicCount = rep.countNrStepAttributes(objectId, "topic");
        for (int i = 0; i < topicCount; i++) {
            addTopic(rep.getStepAttributeString(objectId, i, "topic"));
        }
        setBizType(rep.getStepAttributeString(objectId, "bizType"));
        setBizObjectId(rep.getStepAttributeString(objectId, "bizObjectId"));
        setConsumerGroup(rep.getStepAttributeString(objectId, CONSUMER_GROUP));
        setTransformationPath(rep.getStepAttributeString(objectId, TRANSFORMATION_PATH));
        setSubStep(rep.getStepAttributeString(objectId, "SUB_STEP"));
        setFileName(rep.getStepAttributeString(objectId, TRANSFORMATION_PATH));
        setBatchSize((String) Optional.ofNullable(rep.getStepAttributeString(objectId, BATCH_SIZE)).orElse(""));
        setBatchDuration((String) Optional.ofNullable(rep.getStepAttributeString(objectId, BATCH_DURATION)).orElse(""));
        String parallelism = rep.getStepAttributeString(objectId, "PARALLELISM");
        setParallelism(Strings.isNullOrEmpty(parallelism) ? "1" : parallelism);
        String prefetchCount = rep.getStepAttributeString(objectId, PREFETCH_MESSAGE_COUNT);
        setPrefetchCount(Strings.isNullOrEmpty(prefetchCount) ? PREFETCH_DEFAULT : prefetchCount);
        String connectionTypeRep = rep.getStepAttributeString(objectId, "connectionType");
        try {
            setConnectionType(Strings.isNullOrEmpty(connectionTypeRep) ? ConnectionType.DIRECT : ConnectionType.valueOf(connectionTypeRep));
        } catch (IllegalArgumentException ex) {
            setConnectionType(ConnectionType.DIRECT);
        }
        setDirectBootstrapServers(rep.getStepAttributeString(objectId, "directBootstrapServers"));
        setAutoCommit(rep.getStepAttributeBoolean(objectId, 0, AUTO_COMMIT, true));
        setPartition(rep.getStepAttributeString(objectId, "partition"));
        for (KafkaConsumerField.Name name : KafkaConsumerField.Name.values()) {
            String prefix = "OutputField_" + name;
            String value = rep.getStepAttributeString(objectId, prefix);
            String type = rep.getStepAttributeString(objectId, prefix + "_type");
            if (value != null) {
                setField(new KafkaConsumerField(name, value, KafkaConsumerField.Type.valueOf(type)));
            }
        }
        this.config = new LinkedHashMap();
        for (int i2 = 0; i2 < rep.getStepAttributeInteger(objectId, "advancedConfig_COUNT"); i2++) {
            this.config.put(rep.getStepAttributeString(objectId, i2, "advancedConfig_NAME"), rep.getStepAttributeString(objectId, i2, "advancedConfig_VALUE"));
        }
    }

    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId) throws KettleException {
        rep.saveStepAttribute(transId, stepId, "clusterName", this.clusterName);
        int i = 0;
        for (String topic : this.topics) {
            int i2 = i;
            i++;
            rep.saveStepAttribute(transId, stepId, i2, "topic", topic);
        }
        rep.saveStepAttribute(transId, stepId, "bizType", this.bizType);
        rep.saveStepAttribute(transId, stepId, "bizObjectId", this.bizObjectId);
        rep.saveStepAttribute(transId, stepId, CONSUMER_GROUP, this.consumerGroup);
        rep.saveStepAttribute(transId, stepId, TRANSFORMATION_PATH, this.transformationPath);
        rep.saveStepAttribute(transId, stepId, "SUB_STEP", getSubStep());
        rep.saveStepAttribute(transId, stepId, BATCH_SIZE, this.batchSize);
        rep.saveStepAttribute(transId, stepId, BATCH_DURATION, this.batchDuration);
        rep.saveStepAttribute(transId, stepId, "PARALLELISM", this.parallelism);
        rep.saveStepAttribute(transId, stepId, PREFETCH_MESSAGE_COUNT, this.prefetchCount);
        rep.saveStepAttribute(transId, stepId, "connectionType", this.connectionType.name());
        rep.saveStepAttribute(transId, stepId, "directBootstrapServers", this.directBootstrapServers);
        rep.saveStepAttribute(transId, stepId, AUTO_COMMIT, this.autoCommit);
        rep.saveStepAttribute(transId, stepId, "partition", this.partition);
        List<KafkaConsumerField> fields = getFieldDefinitions();
        for (KafkaConsumerField field : fields) {
            String prefix = "OutputField_" + field.getKafkaName().toString();
            rep.saveStepAttribute(transId, stepId, prefix, field.getOutputName());
            rep.saveStepAttribute(transId, stepId, prefix + "_type", field.getOutputType().toString());
        }
        rep.saveStepAttribute(transId, stepId, "advancedConfig_COUNT", getConfig().size());
        int i3 = 0;
        for (String propName : getConfig().keySet()) {
            rep.saveStepAttribute(transId, stepId, i3, "advancedConfig_NAME", propName);
            int i4 = i3;
            i3++;
            rep.saveStepAttribute(transId, stepId, i4, "advancedConfig_VALUE", getConfig().get(propName));
        }
    }

    public RowMeta getRowMeta(String origin, VariableSpace space) throws KettleStepException {
        RowMeta rowMeta = new RowMeta();
        putFieldOnRowMeta(getKeyField(), rowMeta, origin, space);
        putFieldOnRowMeta(getMessageField(), rowMeta, origin, space);
        putFieldOnRowMeta(getTopicField(), rowMeta, origin, space);
        putFieldOnRowMeta(getPartitionField(), rowMeta, origin, space);
        putFieldOnRowMeta(getOffsetField(), rowMeta, origin, space);
        putFieldOnRowMeta(getTimestampField(), rowMeta, origin, space);
        return rowMeta;
    }

    void putFieldOnRowMeta(KafkaConsumerField field, RowMetaInterface rowMeta, String origin, VariableSpace space) throws KettleStepException {
        if (field != null && !Utils.isEmpty(field.getOutputName())) {
            try {
                String value = space.environmentSubstitute(field.getOutputName());
                ValueMetaInterface v = ValueMetaFactory.createValueMeta(value, field.getOutputType().getValueMetaInterfaceType());
                v.setOrigin(origin);
                rowMeta.addValueMeta(v);
            } catch (KettlePluginException e) {
                throw new KettleStepException(BaseMessages.getString(PKG, "KafkaConsumerInputMeta.UnableToCreateValueType", new Object[]{field}), e);
            }
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans) {
        return new KafkaConsumerInput(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    public StepDataInterface getStepData() {
        return new KafkaConsumerInputData();
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public void addTopic(String topic) {
        this.topics.add(topic);
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    /**
     * 供 {@link KafkaFactory} 使用的引导地址；本工程不依赖 Pentaho Named Cluster，仅使用直连地址
     *（SASL/SSL 等可通过 advancedConfig / 数据源 properties 注入标准 Kafka 客户端参数）。
     */
    public String getBootstrapServers() {
        return getDirectBootstrapServers();
    }

    public List<String> getTopics() {
        return this.topics;
    }

    public String getConsumerGroup() {
        return this.consumerGroup;
    }

    public KafkaConsumerField getKeyField() {
        return this.keyField;
    }

    public KafkaConsumerField getMessageField() {
        return this.messageField;
    }

    public KafkaConsumerField getTopicField() {
        return this.topicField;
    }

    public KafkaConsumerField getOffsetField() {
        return this.offsetField;
    }

    public KafkaConsumerField getPartitionField() {
        return this.partitionField;
    }

    public KafkaConsumerField getTimestampField() {
        return this.timestampField;
    }

    public String getDirectBootstrapServers() {
        return this.directBootstrapServers;
    }

    public void setKeyField(KafkaConsumerField keyField) {
        this.keyField = keyField;
    }

    public void setMessageField(KafkaConsumerField messageField) {
        this.messageField = messageField;
    }

    public void setTopicField(KafkaConsumerField topicField) {
        this.topicField = topicField;
    }

    public void setOffsetField(KafkaConsumerField offsetField) {
        this.offsetField = offsetField;
    }

    public void setPartitionField(KafkaConsumerField partitionField) {
        this.partitionField = partitionField;
    }

    public void setTimestampField(KafkaConsumerField timestampField) {
        this.timestampField = timestampField;
    }

    public void setDirectBootstrapServers(String directBootstrapServers) {
        this.directBootstrapServers = directBootstrapServers;
    }

    public ConnectionType getConnectionType() {
        return this.connectionType;
    }

    public void setConnectionType(ConnectionType connectionType) {
        this.connectionType = connectionType;
    }

    public String getXML() {
        StringBuilder retval = new StringBuilder();
        retval.append("    ").append(XMLHandler.addTagValue("clusterName", this.clusterName));
        getTopics().forEach(topic -> {
            retval.append("    ").append(XMLHandler.addTagValue("topic", topic));
        });
        retval.append("    ").append(XMLHandler.addTagValue("bizType", this.bizType));
        retval.append("    ").append(XMLHandler.addTagValue("bizObjectId", this.bizObjectId));
        retval.append("    ").append(XMLHandler.addTagValue(CONSUMER_GROUP, this.consumerGroup));
        retval.append("    ").append(XMLHandler.addTagValue(TRANSFORMATION_PATH, this.transformationPath));
        retval.append("    ").append(XMLHandler.addTagValue("SUB_STEP", getSubStep()));
        retval.append("    ").append(XMLHandler.addTagValue(BATCH_SIZE, this.batchSize));
        retval.append("    ").append(XMLHandler.addTagValue(BATCH_DURATION, this.batchDuration));
        retval.append("    ").append(XMLHandler.addTagValue("PARALLELISM", this.parallelism));
        retval.append("    ").append(XMLHandler.addTagValue(PREFETCH_MESSAGE_COUNT, this.prefetchCount));
        retval.append("    ").append(XMLHandler.addTagValue("connectionType", this.connectionType.name()));
        retval.append("    ").append(XMLHandler.addTagValue("directBootstrapServers", this.directBootstrapServers));
        retval.append("    ").append(XMLHandler.addTagValue(AUTO_COMMIT, this.autoCommit));
        retval.append("    ").append(XMLHandler.addTagValue("partition", this.partition));
        getFieldDefinitions().forEach(field -> {
            retval.append("    ").append(XMLHandler.addTagValue(OUTPUT_FIELD_TAG_NAME, field.getOutputName(), true, new String[]{KAFKA_NAME_ATTRIBUTE, field.getKafkaName().toString(), "type", field.getOutputType().toString()}));
        });
        retval.append("    ").append(XMLHandler.openTag("advancedConfig")).append(Const.CR);
        getConfig().forEach((key, value) -> {
            retval.append("        ").append(XMLHandler.addTagValue("option", "", true, new String[]{"property", key, "value", value}));
        });
        retval.append("    ").append(XMLHandler.closeTag("advancedConfig")).append(Const.CR);
        return retval.toString();
    }

    public List<KafkaConsumerField> getFieldDefinitions() {
        return Lists.newArrayList(new KafkaConsumerField[]{getKeyField(), getMessageField(), getTopicField(), getPartitionField(), getOffsetField(), getTimestampField()});
    }

    public KafkaFactory getKafkaFactory() {
        return this.kafkaFactory;
    }

    void setKafkaFactory(KafkaFactory kafkaFactory) {
        this.kafkaFactory = kafkaFactory;
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBizType() {
        return this.bizType;
    }

    public void setBizType(String bizType) {
        this.bizType = bizType;
    }

    public String getBizObjectId() {
        return this.bizObjectId;
    }

    public void setBizObjectId(String bizObjectId) {
        this.bizObjectId = bizObjectId;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public boolean isAutoCommit() {
        return this.autoCommit;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Map<String, String> getConfig() {
        applyInjectedProperties();
        return this.config;
    }

    public String getPartition() {
        return this.partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    protected void applyInjectedProperties() {
        if (this.injectedConfigNames != null || this.injectedConfigValues != null) {
            Preconditions.checkState(this.injectedConfigNames != null, "Options names were not injected");
            Preconditions.checkState(this.injectedConfigValues != null, "Options values were not injected");
            Preconditions.checkState(this.injectedConfigNames.size() == this.injectedConfigValues.size(), "Injected different number of options names and value");
            Map<String, String> injected = IntStream.range(0, this.injectedConfigNames.size())
                    .boxed()
                    .collect(Collectors.toMap(
                            this.injectedConfigNames::get,
                            this.injectedConfigValues::get,
                            (a, b) -> a,
                            LinkedHashMap::new));
            setConfig(injected);
            this.injectedConfigNames = null;
            this.injectedConfigValues = null;
        }
    }
}