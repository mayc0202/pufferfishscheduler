package com.pufferfishscheduler.plugin;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.domain.DebeziumField;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Step(id = "DebeziumJson",
        name = "解析Debezium报文",
        description = "解析Debezium CDC JSON报文",
        image = "debezium_json.svg",
        categoryDescription = "转换")
public class DebeziumJsonStepMeta extends BaseStepMeta implements StepMetaInterface {

    private String sampleData;
    private String sourceField;
    private String messageStructure;
    private String[] outputFieldConfig;

    @Override
    public void setDefault() {
        sampleData = "";
        sourceField = "";
        messageStructure = "";
        outputFieldConfig = new String[0];
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta,
                                         TransMeta transMeta, String name) {
        return new DebeziumJsonStepDialog(shell, meta, transMeta, name);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                                 int copyNr, TransMeta transMeta, Trans trans) {
        return new DebeziumJsonStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new DebeziumJsonStepData();
    }

    @Override
    public String getXML() {
        StringBuilder xml = new StringBuilder();
        xml.append("        ").append(XMLHandler.addTagValue(Constants.SAMPLE_DATA, sampleData));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.SOURCE_FIELD, sourceField));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.MESSAGE_STRUCTURE, messageStructure));
        xml.append("    <fields>").append(Const.CR);
        if (outputFieldConfig != null) {
            for (String field : outputFieldConfig) {
                xml.append("      <field>").append(Const.CR);
                xml.append("        ").append(XMLHandler.addTagValue(Constants.OUTPUT_FIELD_CONFIG, field));
                xml.append("      </field>").append(Const.CR);
            }
        }
        xml.append("    </fields>").append(Const.CR);
        return xml.toString();
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId) throws KettleException {
        try {
            rep.saveStepAttribute(transId, stepId, Constants.SAMPLE_DATA, sampleData);
            rep.saveStepAttribute(transId, stepId, Constants.SOURCE_FIELD, sourceField);
            rep.saveStepAttribute(transId, stepId, Constants.MESSAGE_STRUCTURE, messageStructure);
            if (outputFieldConfig != null) {
                for (int i = 0; i < outputFieldConfig.length; i++) {
                    rep.saveStepAttribute(transId, stepId, i, Constants.OUTPUT_FIELD_CONFIG, outputFieldConfig[i]);
                }
            }
        } catch (Exception e) {
            throw new KettleException(Constants.ANALYSIS_JSON_SAVE_FAIL + stepId, e);
        }
    }

    @Override
    public void loadXML(Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        try {
            sampleData = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, Constants.SAMPLE_DATA));
            sourceField = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, Constants.SOURCE_FIELD));
            messageStructure = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepNode, Constants.MESSAGE_STRUCTURE));

            Node fieldsNode = XMLHandler.getSubNode(stepNode, Constants.FIELDS);
            int count = XMLHandler.countNodes(fieldsNode, Constants.FIELD);
            outputFieldConfig = new String[count];
            for (int i = 0; i < count; i++) {
                Node fieldNode = XMLHandler.getSubNodeByNr(fieldsNode, Constants.FIELD, i);
                outputFieldConfig[i] = Const.NVL(XMLHandler.getTagValue(fieldNode, Constants.OUTPUT_FIELD_CONFIG), "");
            }
        } catch (Exception e) {
            throw new KettleXMLException(Constants.ANALYSIS_JSON_READ_FAIL, e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases) throws KettleException {
        try {
            int count = rep.countNrStepAttributes(stepId, Constants.OUTPUT_FIELD_CONFIG);
            outputFieldConfig = new String[count];
            sampleData = rep.getStepAttributeString(stepId, Constants.SAMPLE_DATA);
            sourceField = rep.getStepAttributeString(stepId, Constants.SOURCE_FIELD);
            messageStructure = rep.getStepAttributeString(stepId, Constants.MESSAGE_STRUCTURE);

            for (int i = 0; i < count; i++) {
                outputFieldConfig[i] = Const.NVL(rep.getStepAttributeString(stepId, i, Constants.OUTPUT_FIELD_CONFIG), "");
            }
        } catch (Exception e) {
            throw new KettleException(Constants.ANALYSIS_JSON_READ_FAIL, e);
        }
    }

    @Override
    public Object clone() {
        DebeziumJsonStepMeta clone = (DebeziumJsonStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(clone, this);
            // 深度克隆数组
            if (getOutputFieldConfig() != null) {
                clone.outputFieldConfig = getOutputFieldConfig().clone();
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to clone step metadata", e);
        }
        return clone;
    }

    @Override
    public void getFields(RowMetaInterface outputRowMeta, String stepName,
                          RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) {
        String[] outputFieldConfig = getOutputFieldConfig();
        for (String fieldJson : outputFieldConfig) {
            DebeziumField field = JSONObject.parseObject(fieldJson, DebeziumField.class);
            if (field != null) {
                addValueMeta(outputRowMeta, field);
            }
        }
    }

    private void addValueMeta(RowMetaInterface outputRowMeta, DebeziumField field) {
        try {
            int valueType = mapTypeToKettleType(field.getType(), field.getName());
            ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta(valueType);
            valueMeta.setName(field.getName());
            outputRowMeta.addValueMeta(valueMeta);
        } catch (KettlePluginException e) {
            throw new RuntimeException("Failed to create value meta for field: " + field.getField(), e);
        }
    }

    private int mapTypeToKettleType(String dataType, String schemaName) {
        if (dataType == null) return ValueMetaInterface.TYPE_STRING;
        return switch (dataType) {
            case "float", "double" -> ValueMetaInterface.TYPE_NUMBER;
            case "boolean" -> ValueMetaInterface.TYPE_BOOLEAN;
            case "int", "int16", "int32", "long" -> ValueMetaInterface.TYPE_INTEGER;
            case "int64" -> {
                if (Constants.TIMESTAMP_SCHEMA_NAME.equals(schemaName) ||
                        Constants.ZONED_TIMESTAMP_SCHEMA_NAME.equals(schemaName) ||
                        Constants.APACHE_TIMESTAMP_SCHEMA_NAME.equals(schemaName)) {
                    yield ValueMetaInterface.TYPE_TIMESTAMP;
                }
                yield ValueMetaInterface.TYPE_INTEGER;
            }
            case "binary", "bytes" -> ValueMetaInterface.TYPE_BINARY;
            default -> ValueMetaInterface.TYPE_STRING;
        };
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
                      StepMeta stepMeta, RowMetaInterface prev, String[] input,
                      String[] output, RowMetaInterface info, VariableSpace space,
                      Repository repository, IMetaStore metaStore) {
        if (StringUtils.isBlank(sourceField) || outputFieldConfig == null || outputFieldConfig.length == 0) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING,
                    Constants.ANALYSIS_JSON_CONFIG_ISNULL, stepMeta));
        } else {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
                    Constants.ANALYSIS_JSON_CHECK_SUCCESS, stepMeta));
        }
    }

    // Getters / Setters
    public String getSampleData() { return sampleData; }
    public void setSampleData(String sampleData) { this.sampleData = sampleData; }
    public String getSourceField() { return sourceField; }
    public void setSourceField(String sourceField) { this.sourceField = sourceField; }
    public String getMessageStructure() { return messageStructure; }
    public void setMessageStructure(String messageStructure) { this.messageStructure = messageStructure; }
    public String[] getOutputFieldConfig() { return outputFieldConfig; }
    public void setOutputFieldConfig(String[] outputFieldConfig) { this.outputFieldConfig = outputFieldConfig; }
}