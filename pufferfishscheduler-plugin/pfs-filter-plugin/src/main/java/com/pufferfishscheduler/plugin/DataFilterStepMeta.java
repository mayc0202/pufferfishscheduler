package com.pufferfishscheduler.plugin;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
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
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * 数据过滤步骤元数据
 */
@Step(id = "DataFilter", name = "数据过滤", description = "用于数据过滤。", image = "datafilter.svg", categoryDescription = "转换")
public class DataFilterStepMeta extends BaseStepMeta implements StepMetaInterface {
    private String name;
    private String describe;
    private String filterType;
    private String javaCode;
    private String condition;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescribe() {
        return this.describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    public String getFilterType() {
        return this.filterType;
    }

    public void setFilterType(String filterType) {
        this.filterType = filterType;
    }

    public String getJavaCode() {
        return this.javaCode;
    }

    public void setJavaCode(String javaCode) {
        this.javaCode = javaCode;
    }

    public String getCondition() {
        return this.condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public void allocate(int nrkeys) {
        this.name = "";
        this.describe = "";
        this.javaCode = "";
        this.condition = "";
        this.filterType = "0";
    }

    public void setDefault() {
        allocate(0);
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new DataFilterStepDialog(shell, meta, transMeta, name);
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new DataFilterStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public StepDataInterface getStepData() {
        return new DataFilterStepData();
    }

    public String getXML() throws KettleException {
        StringBuilder retval = new StringBuilder();
        retval.append("        ").append(XMLHandler.addTagValue("name", this.name));
        retval.append("        ").append(XMLHandler.addTagValue("describe", this.describe));
        retval.append("        ").append(XMLHandler.addTagValue("filterType", this.filterType));
        retval.append("        ").append(XMLHandler.addTagValue("javaCode", this.javaCode));
        retval.append("        ").append(XMLHandler.addTagValue("condition", this.condition));
        return retval.toString();
    }

    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "name", this.name);
            rep.saveStepAttribute(id_transformation, id_step, "condition", this.condition);
            rep.saveStepAttribute(id_transformation, id_step, "describe", this.describe);
            rep.saveStepAttribute(id_transformation, id_step, "filterType", this.filterType);
            rep.saveStepAttribute(id_transformation, id_step, "javaCode", this.javaCode);
        } catch (Exception e) {
            throw new KettleException("过滤步骤组件保存失败: " + id_step, e);
        }
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        readData(stepnode);
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            this.name = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "name"));
            this.condition = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "condition"));
            this.describe = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "describe"));
            this.filterType = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "filterType"));
            this.javaCode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "javaCode"));
        } catch (Exception var6) {
            throw new KettleXMLException("过滤步骤组件读取失败！", var6);
        }
    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        try {
            int nrkeys = rep.countNrStepAttributes(id_step, "field_name");
            allocate(nrkeys);
            this.name = rep.getStepAttributeString(id_step, "name");
            this.condition = rep.getStepAttributeString(id_step, "condition");
            this.describe = rep.getStepAttributeString(id_step, "describe");
            this.filterType = rep.getStepAttributeString(id_step, "filterType");
            this.javaCode = rep.getStepAttributeString(id_step, "javaCode");
        } catch (Exception var7) {
            throw new KettleException("过滤步骤组件读取失败！", var7);
        }
    }

    @Override
    public Object clone() {
        DataFilterStepMeta retval = (DataFilterStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(retval, this);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return retval;
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore) {
        CheckResult cr;
        if (this.filterType == null) {
            cr = new CheckResult(4, "过滤配置为空！", stepMeta);
        } else {
            cr = new CheckResult(1, "校验通过！", stepMeta);
        }
        remarks.add(cr);
    }
}
