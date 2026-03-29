package com.pufferfishscheduler.plugin;

import com.pufferfishscheduler.plugin.common.Constants;
import org.apache.commons.beanutils.BeanUtils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
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

@Step(
        id = "DataClean",
        name = "数据清洗",
        description = "用于数据清洗。",
        image = "dataclean.svg",
        categoryDescription = "转换"
)
public class DataCleanStepMeta extends BaseStepMeta implements StepMetaInterface {

    private String name;
    private String describe;
    private String[] fieldName;
    private String[] fieldDescription;
    private String[] dataType;
    private String[] cleanType;
    private String[] processorClass;
    private String[] params;
    private String[] rename;
    private String[] renameType;
    private String[] ruleId;
    private String address;
    private String apiKey;
    private String apiRoute;

    // ✅ 修复1: 添加无参构造函数（Kettle框架通过反射 Class.newInstance() 实例化，必须存在）
    public DataCleanStepMeta() {
        super();
    }

    public DataCleanStepMeta(String name, String describe, String[] fieldName, String[] fieldDescription,
                             String[] dataType, String[] cleanType, String[] processorClass,
                             String[] params, String[] rename, String[] renameType,
                             String[] ruleId, String address, String apiKey, String apiRoute) {
        this.name = name;
        this.describe = describe;
        this.fieldName = fieldName;
        this.fieldDescription = fieldDescription;
        this.dataType = dataType;
        this.cleanType = cleanType;
        this.processorClass = processorClass;
        this.params = params;
        this.rename = rename;
        this.renameType = renameType;
        this.ruleId = ruleId;
        this.address = address;
        this.apiKey = apiKey;
        this.apiRoute = apiRoute;
    }

    // ===================== Getters & Setters =====================

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescribe() { return describe; }
    public void setDescribe(String describe) { this.describe = describe; }
    public String[] getFieldName() { return fieldName; }
    public void setFieldName(String[] fieldName) { this.fieldName = fieldName; }
    public String[] getFieldDescription() { return fieldDescription; }
    public void setFieldDescription(String[] fieldDescription) { this.fieldDescription = fieldDescription; }
    public String[] getDataType() { return dataType; }
    public void setDataType(String[] dataType) { this.dataType = dataType; }
    public String[] getCleanType() { return cleanType; }
    public void setCleanType(String[] cleanType) { this.cleanType = cleanType; }
    public String[] getProcessorClass() { return processorClass; }
    public void setProcessorClass(String[] processorClass) { this.processorClass = processorClass; }
    public String[] getParams() { return params; }
    public void setParams(String[] params) { this.params = params; }
    public String[] getRename() { return rename; }
    public void setRename(String[] rename) { this.rename = rename; }
    public String[] getRenameType() { return renameType; }
    public void setRenameType(String[] renameType) { this.renameType = renameType; }
    public String[] getRuleId() { return ruleId; }
    public void setRuleId(String[] ruleId) { this.ruleId = ruleId; }
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    public String getApiKey() { return apiKey; }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }
    public String getApiRoute() { return apiRoute; }
    public void setApiRoute(String apiRoute) { this.apiRoute = apiRoute; }

    // ===================== 核心方法 =====================

    public void allocate(int nrkeys) {
        this.name = "";
        this.describe = "";
        this.fieldName = new String[nrkeys];
        this.fieldDescription = new String[nrkeys];
        this.dataType = new String[nrkeys];
        this.cleanType = new String[nrkeys];
        this.processorClass = new String[nrkeys];
        this.params = new String[nrkeys];
        this.rename = new String[nrkeys];
        this.renameType = new String[nrkeys];
        this.ruleId = new String[nrkeys];
        this.address = "";
        this.apiKey = "";
        this.apiRoute = "";
    }

    @Override
    public void setDefault() {
        this.allocate(0);
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String
            name) {
        return new DataCleanStepDialog(shell, meta, transMeta, name);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                                 int i, TransMeta transMeta, Trans trans) {
        return new DataCleanStep(stepMeta, stepDataInterface, i, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new DataCleanStepData();
    }

    // ===================== XML 序列化（文件方式） =====================

    @Override
    public String getXML() throws KettleException {
        StringBuilder retval = new StringBuilder();
        retval.append("        ").append(XMLHandler.addTagValue(Constants.NAME, this.name));
        retval.append("        ").append(XMLHandler.addTagValue(Constants.DESCRIBE, this.describe));
        retval.append("        ").append(XMLHandler.addTagValue(Constants.ADDRESS, this.address));
        retval.append("        ").append(XMLHandler.addTagValue(Constants.APIKEY, this.apiKey));
        retval.append("        ").append(XMLHandler.addTagValue(Constants.APIROUTE, this.apiRoute));
        retval.append("    <fields>").append(Const.CR);
        for (int i = 0; i < this.fieldName.length; ++i) {
            retval.append("      <field>").append(Const.CR);
            retval.append("        ").append(XMLHandler.addTagValue(Constants.FIELD_NAME,
                    this.fieldName[i]));
            retval.append("        ").append(XMLHandler.addTagValue(Constants.PARAMS, this.params[i]));
            retval.append("        ").append(XMLHandler.addTagValue(Constants.RENAME, this.rename[i]));
            retval.append("        ").append(XMLHandler.addTagValue(Constants.RENAME_TYPE,
                    this.renameType[i]));
            retval.append("        ").append(XMLHandler.addTagValue(Constants.RULEID, this.ruleId[i]));
            retval.append("      </field>").append(Const.CR);
        }
        retval.append("    </fields>").append(Const.CR);
        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
            throws KettleXMLException {
        this.readData(stepnode);
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            this.name    = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.NAME));
            this.describe = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.DESCRIBE));
            this.address = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.ADDRESS));
            this.apiKey  = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.APIKEY));
            this.apiRoute = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.APIROUTE));

            Node lookup = XMLHandler.getSubNode(stepnode, Constants.FIELDS);
            int nrkeys = XMLHandler.countNodes(lookup, Constants.FIELD);
            this.fieldName       = new String[nrkeys];
            this.fieldDescription = new String[nrkeys];
            this.dataType        = new String[nrkeys];
            this.cleanType       = new String[nrkeys];
            this.processorClass  = new String[nrkeys];
            this.params          = new String[nrkeys];
            this.rename          = new String[nrkeys];
            this.renameType      = new String[nrkeys];
            this.ruleId          = new String[nrkeys];

            for (int i = 0; i < nrkeys; ++i) {
                Node fnode = XMLHandler.getSubNodeByNr(lookup, Constants.FIELD, i);
                this.fieldName[i]  = Const.NVL(XMLHandler.getTagValue(fnode, Constants.FIELD_NAME), "");
                this.params[i]     = Const.NVL(XMLHandler.getTagValue(fnode, Constants.PARAMS), "");
                this.rename[i]     = Const.NVL(XMLHandler.getTagValue(fnode, Constants.RENAME), "");
                this.renameType[i] = Const.NVL(XMLHandler.getTagValue(fnode, Constants.RENAME_TYPE), "");
                this.ruleId[i]     = Const.NVL(XMLHandler.getTagValue(fnode, Constants.RULEID), "");
            }
        } catch (Exception e) {
            throw new KettleXMLException(Constants.DATA_CLEAN_READ_FAIL, e);
        }
    }

    // ===================== Repository 序列化（库方式） =====================

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore,
                        ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, Constants.NAME,    this.name);
            rep.saveStepAttribute(id_transformation, id_step, Constants.DESCRIBE, this.describe);
            rep.saveStepAttribute(id_transformation, id_step, Constants.ADDRESS,  this.address);
            rep.saveStepAttribute(id_transformation, id_step, Constants.APIKEY,   this.apiKey);
            rep.saveStepAttribute(id_transformation, id_step, Constants.APIROUTE, this.apiRoute);

            for (int i = 0; i < this.fieldName.length; ++i) {
                rep.saveStepAttribute(id_transformation, id_step, i, Constants.FIELD_NAME,
                        this.fieldName[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, Constants.PARAMS,      this.params[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, Constants.RENAME,      this.rename[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, Constants.RENAME_TYPE,
                        this.renameType[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, Constants.RULEID,      this.ruleId[i]);
            }
        } catch (Exception e) {
            throw new KettleException(Constants.DATA_CLEAN_SAVE_FAIL + id_step, e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore,
                        ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        try {
            int nrkeys = rep.countNrStepAttributes(id_step, Constants.FIELD_NAME);
            this.allocate(nrkeys);

            this.name    = rep.getStepAttributeString(id_step, Constants.NAME);
            this.describe = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.address = rep.getStepAttributeString(id_step, Constants.ADDRESS);
            this.apiKey  = rep.getStepAttributeString(id_step, Constants.APIKEY);
            this.apiRoute = rep.getStepAttributeString(id_step, Constants.APIROUTE);

            for (int i = 0; i < nrkeys; ++i) {
                this.fieldName[i]  = Const.NVL(rep.getStepAttributeString(id_step, i, Constants.FIELD_NAME),
                        "");
                this.params[i]     = Const.NVL(rep.getStepAttributeString(id_step, i, Constants.PARAMS),
                        "");
                this.rename[i]     = Const.NVL(rep.getStepAttributeString(id_step, i, Constants.RENAME),
                        "");
                this.renameType[i] = Const.NVL(rep.getStepAttributeString(id_step, i,
                        Constants.RENAME_TYPE), "");
                this.ruleId[i]     = Const.NVL(rep.getStepAttributeString(id_step, i, Constants.RULEID),
                        "");
            }
        } catch (Exception e) {
            throw new KettleException(Constants.DATA_CLEAN_READ_FAIL, e);
        }
    }

    // ===================== 其他覆写方法 =====================

    @Override
    public Object clone() {
        DataCleanStepMeta retval = (DataCleanStepMeta) super.clone();
        // ✅ 修复2: BeanUtils.copyProperties(dest, orig) — 第一个参数是目标对象
        // 原代码写反了：copyProperties(this, retval) 是把空克隆覆盖回原对象
        try {
            BeanUtils.copyProperties(retval, this);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return retval;
    }

    @Override
    public void getFields(RowMetaInterface outputRowMeta, String name, RowMetaInterface[] info,
                          StepMeta nextStep, VariableSpace space,
                          Repository repository, IMetaStore metaStore) throws KettleStepException {
        if (this.rename == null) {
            return;
        }
        for (int i = 0; i < this.rename.length; i++) {
            // ✅ 修复3: 跳过空rename，避免添加 name=null 的 ValueMeta 导致下游 NPE
            if (this.rename[i] == null || this.rename[i].isEmpty()) {
                continue;
            }
            try {
                // ✅ 修复4: createValueMeta(name, type) 同时指定名称，避免 name 为 null
                ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta(
                        this.rename[i],
                        this.getDataType(this.renameType[i])
                );
                outputRowMeta.addValueMeta(valueMeta);
            } catch (KettlePluginException e) {
                throw new KettleStepException(e.getMessage(), e);
            }
        }
    }

    /**
     * 将字符串类型名映射为 Kettle ValueMetaInterface 类型常量
     */
    public int getDataType(String dataType) {
        if (dataType == null || dataType.isEmpty()) {
            return ValueMetaInterface.TYPE_STRING; // 默认 String
        }
        switch (dataType) {
            case "-":                         return ValueMetaInterface.TYPE_NONE;
            case Constants.NUMBER:            return ValueMetaInterface.TYPE_NUMBER;
            case Constants.STRING:            return ValueMetaInterface.TYPE_STRING;
            case Constants.DATE:              return ValueMetaInterface.TYPE_DATE;
            case Constants.BOOLEAN:           return ValueMetaInterface.TYPE_BOOLEAN;
            case Constants.INTEGER:           return ValueMetaInterface.TYPE_INTEGER;
            case Constants.BIGNUMBER:         return ValueMetaInterface.TYPE_BIGNUMBER;
            case Constants.BINARY:            return ValueMetaInterface.TYPE_BINARY;
            case Constants.TIMESTAMP:         return ValueMetaInterface.TYPE_TIMESTAMP;
            case Constants.INTERNET_ADDRESS:  return ValueMetaInterface.TYPE_INET;
            default:                          return ValueMetaInterface.TYPE_STRING;
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                      RowMetaInterface prev, String[] input, String[] output,
                      RowMetaInterface info, VariableSpace space,
                      Repository repository, IMetaStore metaStore) {
        CheckResult cr;
        if (this.fieldName == null || this.fieldName.length == 0) {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    Constants.DATA_CLEAN_CONFIG_ISNULL, stepMeta);
        } else {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
                    Constants.CHECK_DATA_CLEAN_SUCCESS, stepMeta);
        }
        remarks.add(cr);
    }

    @Override
    public boolean supportsErrorHandling() {
        return true;
    }
}
