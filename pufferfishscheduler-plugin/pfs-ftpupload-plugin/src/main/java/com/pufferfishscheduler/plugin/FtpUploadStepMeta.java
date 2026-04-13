package com.pufferfishscheduler.plugin;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.FTPConstants;
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
        id = "FTPUpload",
        name = "FTP上传",
        description = "ftp上传",
        image = "ftp-upload.svg",
        categoryDescription = "转换"
)
public class FtpUploadStepMeta extends BaseStepMeta implements StepMetaInterface {

    // 步骤名称
    private String stepName;

    // 说明
    private String description;

    // ip
    private String host;

    // 端口
    private Long port;

    // 用户名
    private String username;

    // 密码
    private String password;

    // 工作模式
    private String mode;

    // 类型
    private String ftpType;

    // 控制编码
    private String controlEncoding;

    // 本地目录
    private String localDirectory;

    // 通配符
    private String wildcard;

    // 远程目录
    private String remoteDirectory;

    // 输出字段配置
    private String[] outputFieldList;

    // 二进制模式
    private String binaryMode;

    // 超时时间 ms
    private Long timeout;

    // 上传后删除本地文件
    private String removeLocalFile;

    // 不覆盖文件
    private String overwriteFile;

    /**
     * 集成内置目录
     */
    private String rootPath;

    /**
     * 组件初始化数据
     */
    @Override
    public void setDefault() {
        int nrkeys = 0;
        this.allocate(nrkeys);
    }

    public void allocate(int nrkeys) {

        this.host = "";
        this.port = 0l;
        this.username = "";
        this.password = "";
        this.mode = "";
        this.controlEncoding = "";
        this.ftpType = "";
        this.stepName = "";
        this.description = "";
        this.localDirectory = "";
        this.wildcard = "";
        this.remoteDirectory = "";
        this.removeLocalFile = "";
        this.timeout = 0l;
        this.binaryMode = "";
        this.overwriteFile = "";
        this.rootPath = "";
        this.outputFieldList = new String[nrkeys];
    }

    /**
     * 桌面设计器引擎会调用此方法获取插件的Dialog对象
     *
     * @param shell
     * @param meta
     * @param transMeta
     * @param name
     * @return
     */
    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new FtpUploadStepDialog(shell, meta, transMeta, name);
    }

    /**
     * 桌面设计器引擎调用此方法获取数据处理类对象
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param i
     * @param transMeta
     * @param trans
     * @return
     */
    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int i,
                                 TransMeta transMeta, Trans trans) {
        return new FtpUploadStep(stepMeta, stepDataInterface, i, transMeta, trans);
    }

    /**
     * 桌面设计器引擎调用此方法获取插件缓存数据类。
     */
    public StepDataInterface getStepData() {
        return new FtpUploadStepData();
    }

    /**
     * 将转换流程从设计器中导出成文件时，调用此方法
     * 使用此 org.pentaho.di.core.xml.XMLHandler 类来处理xml
     */
    @Override
    public String getXML() {
        StringBuilder xml = new StringBuilder();
        xml.append("        ").append(XMLHandler.addTagValue(Constants.HOST, this.host));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.PORT, this.port));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.USERNAME, this.username));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.PASSWORD, this.password));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.MODE, this.mode));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.CONTROL_ENCODING, this.controlEncoding));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.FTP_TYPE, this.ftpType));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.STEP_NAME, this.stepName));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.DESCRIBE, this.description));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.LOCAL_DIRECTORY, this.localDirectory));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.WILDCARD, this.wildcard));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.REMOTE_DIRECTORY, this.remoteDirectory));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.BINARY_MODE, this.binaryMode));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.TIMEOUT, this.timeout));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.REMOVER_LOCAL_FILE, this.removeLocalFile));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.OVERWRITE_FILE, this.overwriteFile));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.ROOT_PATH, this.rootPath));
        xml.append("    <fields>").append(Const.CR);
        for (int i = 0; i < this.outputFieldList.length; ++i) {
            xml.append("      <field>").append(Const.CR);
            xml.append("        ").append(XMLHandler.addTagValue(Constants.OUTPUT_FILE_LIST, this.outputFieldList[i]));
            xml.append("      </field>").append(Const.CR);
        }
        xml.append("    </fields>").append(Const.CR);
        return xml.toString();
    }

    /**
     * 将转换流程保存到设计库中时，调用此方法
     *
     * @param rep
     * @param metaStore
     * @param id_transformation
     * @param id_step
     * @throws KettleException
     */
    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
            throws KettleException {
        try {

            rep.saveStepAttribute(id_transformation, id_step, Constants.STEP_NAME, this.stepName);
            rep.saveStepAttribute(id_transformation, id_step, Constants.DESCRIBE, this.description);
            rep.saveStepAttribute(id_transformation, id_step, Constants.HOST, this.host);
            rep.saveStepAttribute(id_transformation, id_step, Constants.PORT, this.port);
            rep.saveStepAttribute(id_transformation, id_step, Constants.USERNAME, this.username);
            rep.saveStepAttribute(id_transformation, id_step, Constants.PASSWORD, this.password);
            rep.saveStepAttribute(id_transformation, id_step, Constants.MODE, this.mode);
            rep.saveStepAttribute(id_transformation, id_step, Constants.CONTROL_ENCODING, this.controlEncoding);
            rep.saveStepAttribute(id_transformation, id_step, Constants.FTP_TYPE, this.ftpType);
            rep.saveStepAttribute(id_transformation, id_step, Constants.LOCAL_DIRECTORY, this.localDirectory);
            rep.saveStepAttribute(id_transformation, id_step, Constants.WILDCARD, this.wildcard);
            rep.saveStepAttribute(id_transformation, id_step, Constants.REMOTE_DIRECTORY, this.remoteDirectory);
            rep.saveStepAttribute(id_transformation, id_step, Constants.BINARY_MODE, this.binaryMode);
            rep.saveStepAttribute(id_transformation, id_step, Constants.TIMEOUT, this.timeout);
            rep.saveStepAttribute(id_transformation, id_step, Constants.REMOVER_LOCAL_FILE, this.removeLocalFile);
            rep.saveStepAttribute(id_transformation, id_step, Constants.OVERWRITE_FILE, this.overwriteFile);
            rep.saveStepAttribute(id_transformation, id_step, Constants.ROOT_PATH, this.rootPath);
            for (int i = 0; i < this.outputFieldList.length; ++i) {
                rep.saveStepAttribute(id_transformation, id_step, i, Constants.OUTPUT_FILE_LIST, this.outputFieldList[i]);
            }
        } catch (Exception e) {
            throw new KettleException(Constants.SAVE_FAIL + id_step, e);
        }
    }

    /**
     * 将转换流程从文件导入桌面设计器时，引擎会调用此方法
     * 使用此 org.pentaho.di.core.xml.XMLHandler 类来处理xml
     *
     * @param stepnode
     * @param databases
     * @param metaStore
     * @throws KettleXMLException
     */
    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        this.readData(stepnode);
    }

    /**
     * 读数据
     *
     * @param stepnode
     * @throws KettleXMLException
     */
    private void readData(Node stepnode) throws KettleXMLException {
        try {

            this.stepName = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.STEP_NAME));
            this.description = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.DESCRIBE));
            this.host = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.HOST));
            this.port = Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.PORT)));
            this.username = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.USERNAME));
            this.password = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.PASSWORD));
            this.mode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.MODE));
            this.controlEncoding = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.CONTROL_ENCODING));
            this.ftpType = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.FTP_TYPE));
            this.localDirectory = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.LOCAL_DIRECTORY));
            this.wildcard = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.WILDCARD));
            this.remoteDirectory = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.REMOTE_DIRECTORY));
            this.binaryMode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.BINARY_MODE));
            this.timeout = Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.TIMEOUT)));
            this.removeLocalFile = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.REMOVER_LOCAL_FILE));
            this.overwriteFile = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.OVERWRITE_FILE));
            this.rootPath = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.ROOT_PATH));

            Node lookup = XMLHandler.getSubNode(stepnode, Constants.FIELDS);
            int nrkeys = XMLHandler.countNodes(lookup, Constants.FIELD);
            this.outputFieldList = new String[nrkeys];
            for (int i = 0; i < nrkeys; ++i) {
                Node fnode = XMLHandler.getSubNodeByNr(lookup, Constants.FIELD, i);
                this.outputFieldList[i] = Const.NVL(XMLHandler.getTagValue(fnode, Constants.OUTPUT_FILE_LIST), "");
            }
        } catch (Exception var6) {
            throw new KettleXMLException(Constants.READ_FAIL, var6);
        }
    }

    /**
     * 从设计库中读取转换流程时，引擎调用此方法
     *
     * @param rep
     * @param metaStore
     * @param id_step
     * @param databases
     * @throws KettleException
     */
    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
            throws KettleException {
        try {
            int nrkeys = rep.countNrStepAttributes(id_step, Constants.OUTPUT_FILE_LIST);
            this.allocate(nrkeys);

            this.stepName = rep.getStepAttributeString(id_step, Constants.STEP_NAME);
            this.description = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.host = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.port = rep.getStepAttributeInteger(id_step, Constants.DESCRIBE);
            this.username = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.password = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.mode = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.controlEncoding = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.ftpType = rep.getStepAttributeString(id_step, Constants.FTP_TYPE);
            this.localDirectory = rep.getStepAttributeString(id_step, Constants.LOCAL_DIRECTORY);
            this.wildcard = rep.getStepAttributeString(id_step, Constants.WILDCARD);
            this.remoteDirectory = rep.getStepAttributeString(id_step, Constants.REMOTE_DIRECTORY);
            this.binaryMode = rep.getStepAttributeString(id_step, Constants.BINARY_MODE);
            this.timeout = rep.getStepAttributeInteger(id_step, Constants.TIMEOUT);
            this.removeLocalFile = rep.getStepAttributeString(id_step, Constants.REMOVER_LOCAL_FILE);
            this.overwriteFile = rep.getStepAttributeString(id_step, Constants.OVERWRITE_FILE);
            this.rootPath = rep.getStepAttributeString(id_step, Constants.ROOT_PATH);

            for (int i = 0; i < nrkeys; ++i) {
                this.outputFieldList[i] = Const.NVL(rep.getStepAttributeString(id_step, i, Constants.OUTPUT_FILE_LIST), "");
            }
        } catch (Exception e) {
            throw new KettleException(Constants.READ_FAIL, e);
        }
    }

    /**
     * 在桌面设计器中，如果复制此组件，那么会调用此clone方法，生成组件新的对象。
     * 可参考：org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() 的克隆方法
     */
    @Override
    public Object clone() {
        FtpUploadStepMeta retval = (FtpUploadStepMeta) super.clone();
        try {
            BeanUtils.copyProperties(retval, this);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return retval;
    }

    /**
     * 如果有新生成的输出字段，那么需要在此类中定义。这样在后面的Step组件中才能获取到此字段元数据
     *
     * @param outputRowMeta
     * @param name
     * @param info
     * @param nextStep
     * @param space
     * @param repository
     * @param metaStore
     * @throws KettleStepException
     */
    @Override
    public void getFields(RowMetaInterface outputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) {
        String[] getOutputFieldList = this.getOutputFieldList();
        for (String outputField : getOutputFieldList) {
            JSONObject fieldJson = JSONObject.parseObject(outputField);
            if (fieldJson != null) {
                setValueMeta(outputRowMeta, fieldJson.getString(Constants.FIELD));
            }
        }
    }

    /**
     * 设置输出字段
     *
     * @param outputRowMeta
     * @param field
     * @throws KettleException
     */
    private void setValueMeta(RowMetaInterface outputRowMeta, String field) {
        try {
            ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta(2);
            valueMeta.setName(field);
            outputRowMeta.addValueMeta(valueMeta);
        } catch (KettlePluginException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 在设计器中点击“校验流程”功能时，会调用此方法。校验该转换步骤配置是否正确
     *
     * @param remarks
     * @param transMeta
     * @param stepMeta
     * @param prev
     * @param input
     * @param output
     * @param info
     * @param space
     * @param repository
     * @param metaStore
     */
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info,
                      VariableSpace space, Repository repository, IMetaStore metaStore) {
        CheckResult cr;
        if (host == null || port == null || username == null || password == null ||
                mode == null || controlEncoding == null || localDirectory == null ||
                wildcard == null || remoteDirectory == null || outputFieldList.length == 0
                || ftpType == null || rootPath == null) {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, Constants.CONFIG_ISNULL, stepMeta);
            remarks.add(cr);
        } else {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, Constants.CHECK_SUCCESS, stepMeta);
            remarks.add(cr);
        }
    }

    public FtpUploadStepMeta() {

    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getLocalDirectory() {
        return localDirectory;
    }

    public void setLocalDirectory(String localDirectory) {
        this.localDirectory = localDirectory;
    }

    public String getWildcard() {
        return wildcard;
    }

    public void setWildcard(String wildcard) {
        this.wildcard = wildcard;
    }

    public String getRemoteDirectory() {
        return remoteDirectory;
    }

    public void setRemoteDirectory(String remoteDirectory) {
        this.remoteDirectory = remoteDirectory;
    }

    public String[] getOutputFieldList() {
        return outputFieldList;
    }

    public void setOutputFieldList(String[] outputFieldList) {
        this.outputFieldList = outputFieldList;
    }

    public String getBinaryMode() {
        return binaryMode;
    }

    public void setBinaryMode(String binaryMode) {
        this.binaryMode = binaryMode;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public String getRemoveLocalFile() {
        return removeLocalFile;
    }

    public void setRemoveLocalFile(String removeLocalFile) {
        this.removeLocalFile = removeLocalFile;
    }

    public String getOverwriteFile() {
        return overwriteFile;
    }

    public void setOverwriteFile(String overwriteFile) {
        this.overwriteFile = overwriteFile;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Long getPort() {
        return port;
    }

    public void setPort(Long port) {
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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getControlEncoding() {
        return !"".equals(controlEncoding) ? FTPConstants.CONTROL_ENCODING_UTF : controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public String getFtpType() {
        return ftpType;
    }

    public void setFtpType(String ftpType) {
        this.ftpType = ftpType;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }
}
