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

/**
 * 此插件将输入的流中的第一列数据加上页面配置的指定字符串值并生成新的字符串类型的输出字段（outputfieldname）
 *
 * @author zhangweilai
 */
@Step(
        id = "FTPDownload",
        name = "ftp下载",
        description = "ftp下载",
        image = "ftp-download.svg",
        categoryDescription = "转换"
)
public class FtpDownloadStepMeta extends BaseStepMeta implements StepMetaInterface {

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

    // 数据源Id
    private Long dataSourceId;

    // 远程目录
    private String remoteDirectory;

    // 输出字段配置
    private String[] outputFieldList;

    // 二进制模式
    private String binaryMode;

    // 超时时间 ms
    private Long timeout;

    // 获取后删除文件
    private String removeFile;

    // 文件名中包含日期
    private String dateInFilename;

    // 日期格式
    private String dateTimeFormat;

    // 获取后移动文件
    private String moveFile;

    // 移动到文件夹
    private String moveToDirectory;

    // 移动文件夹不存在自动创建
    private String createNewFolder;


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
        this.dataSourceId = 0l;
        this.remoteDirectory = "";
        this.removeFile = "";
        this.timeout = 0l;
        this.binaryMode = "";
        this.dateTimeFormat = "";
        this.moveFile = "";
        this.moveToDirectory = "";
        this.createNewFolder = "";
        this.dateInFilename = "";
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
        return new FtpDownloadStepDialog(shell, meta, transMeta, name);
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
        return new FtpDownloadStep(stepMeta, stepDataInterface, i, transMeta, trans);
    }

    /**
     * 桌面设计器引擎调用此方法获取插件缓存数据类。
     */
    public StepDataInterface getStepData() {
        return new FtpDownloadStepData();
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
        xml.append("        ").append(XMLHandler.addTagValue(Constants.DATA_SOURCE_ID, this.dataSourceId));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.REMOTE_DIRECTORY, this.remoteDirectory));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.BINARY_MODE, this.binaryMode));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.TIMEOUT, this.timeout));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.REMOVER_FILE, this.removeFile));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.MOVE_FILE, this.moveFile));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.DATE_TIME_FORMAT, this.dateTimeFormat));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.MOVE_TO_DIRECTORY, this.moveToDirectory));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.CREATE_NEW_FOLDER, this.createNewFolder));
        xml.append("        ").append(XMLHandler.addTagValue(Constants.DATE_IN_FILE_NAME, this.dateInFilename));
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
            rep.saveStepAttribute(id_transformation, id_step, Constants.HOST, this.host);
            rep.saveStepAttribute(id_transformation, id_step, Constants.PORT, this.port);
            rep.saveStepAttribute(id_transformation, id_step, Constants.USERNAME, this.username);
            rep.saveStepAttribute(id_transformation, id_step, Constants.PASSWORD, this.password);
            rep.saveStepAttribute(id_transformation, id_step, Constants.MODE, this.mode);
            rep.saveStepAttribute(id_transformation, id_step, Constants.CONTROL_ENCODING, this.controlEncoding);
            rep.saveStepAttribute(id_transformation, id_step, Constants.FTP_TYPE, this.ftpType);
            rep.saveStepAttribute(id_transformation, id_step, Constants.STEP_NAME, this.stepName);
            rep.saveStepAttribute(id_transformation, id_step, Constants.DESCRIBE, this.description);
            rep.saveStepAttribute(id_transformation, id_step, Constants.LOCAL_DIRECTORY, this.localDirectory);
            rep.saveStepAttribute(id_transformation, id_step, Constants.WILDCARD, this.wildcard);
            rep.saveStepAttribute(id_transformation, id_step, Constants.DATA_SOURCE_ID, this.dataSourceId);
            rep.saveStepAttribute(id_transformation, id_step, Constants.REMOTE_DIRECTORY, this.remoteDirectory);
            rep.saveStepAttribute(id_transformation, id_step, Constants.BINARY_MODE, this.binaryMode);
            rep.saveStepAttribute(id_transformation, id_step, Constants.TIMEOUT, this.timeout);
            rep.saveStepAttribute(id_transformation, id_step, Constants.REMOVER_FILE, this.removeFile);
            rep.saveStepAttribute(id_transformation, id_step, Constants.MOVE_FILE, this.moveFile);
            rep.saveStepAttribute(id_transformation, id_step, Constants.DATE_TIME_FORMAT, this.dateTimeFormat);
            rep.saveStepAttribute(id_transformation, id_step, Constants.MOVE_TO_DIRECTORY, this.moveToDirectory);
            rep.saveStepAttribute(id_transformation, id_step, Constants.CREATE_NEW_FOLDER, this.createNewFolder);
            rep.saveStepAttribute(id_transformation, id_step, Constants.DATE_IN_FILE_NAME, this.dateInFilename);
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
            this.host = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.HOST));
            this.port = Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.PORT)));
            this.username = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.USERNAME));
            this.password = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.PASSWORD));
            this.mode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.MODE));
            this.controlEncoding = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.CONTROL_ENCODING));
            this.ftpType = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.FTP_TYPE));
            this.stepName = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.STEP_NAME));
            this.description = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.DESCRIBE));
            this.localDirectory = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.LOCAL_DIRECTORY));
            this.wildcard = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.WILDCARD));
            this.dataSourceId = Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.DATA_SOURCE_ID)));
            this.remoteDirectory = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.REMOTE_DIRECTORY));
            this.binaryMode = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.BINARY_MODE));
            this.timeout = Long.parseLong(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.TIMEOUT)));
            this.removeFile = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.REMOVER_FILE));
            this.moveFile = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.MOVE_FILE));
            this.dateTimeFormat = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.DATE_TIME_FORMAT));
            this.moveToDirectory = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.MOVE_TO_DIRECTORY));
            this.createNewFolder = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.CREATE_NEW_FOLDER));
            this.dateInFilename = XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, Constants.DATE_IN_FILE_NAME));
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
            this.host = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.port = rep.getStepAttributeInteger(id_step, Constants.DESCRIBE);
            this.username = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.password = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.mode = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.controlEncoding = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.ftpType = rep.getStepAttributeString(id_step, Constants.FTP_TYPE);
            this.stepName = rep.getStepAttributeString(id_step, Constants.STEP_NAME);
            this.description = rep.getStepAttributeString(id_step, Constants.DESCRIBE);
            this.localDirectory = rep.getStepAttributeString(id_step, Constants.LOCAL_DIRECTORY);
            this.wildcard = rep.getStepAttributeString(id_step, Constants.WILDCARD);
            this.dataSourceId = rep.getStepAttributeInteger(id_step, Constants.DATA_SOURCE_ID);
            this.remoteDirectory = rep.getStepAttributeString(id_step, Constants.REMOTE_DIRECTORY);
            this.binaryMode = rep.getStepAttributeString(id_step, Constants.BINARY_MODE);
            this.timeout = rep.getStepAttributeInteger(id_step, Constants.TIMEOUT);
            this.removeFile = rep.getStepAttributeString(id_step, Constants.REMOVER_FILE);
            this.moveFile = rep.getStepAttributeString(id_step, Constants.MOVE_FILE);
            this.dateTimeFormat = rep.getStepAttributeString(id_step, Constants.DATE_TIME_FORMAT);
            this.moveToDirectory = rep.getStepAttributeString(id_step, Constants.MOVE_TO_DIRECTORY);
            this.createNewFolder = rep.getStepAttributeString(id_step, Constants.CREATE_NEW_FOLDER);
            this.dateInFilename = rep.getStepAttributeString(id_step, Constants.DATE_IN_FILE_NAME);
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
        FtpDownloadStepMeta retval = (FtpDownloadStepMeta) super.clone();
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

    public FtpDownloadStepMeta() {

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

    public Long getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(Long dataSourceId) {
        this.dataSourceId = dataSourceId;
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

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getRemoveFile() {
        return removeFile;
    }

    public void setRemoveFile(String removeFile) {
        this.removeFile = removeFile;
    }

    public String getDateTimeFormat() {
        return dateTimeFormat;
    }

    public void setDateTimeFormat(String dateTimeFormat) {
        this.dateTimeFormat = dateTimeFormat;
    }

    public String getMoveFile() {
        return moveFile;
    }

    public void setMoveFile(String moveFile) {
        this.moveFile = moveFile;
    }

    public String getMoveToDirectory() {
        return moveToDirectory;
    }

    public void setMoveToDirectory(String moveToDirectory) {
        this.moveToDirectory = moveToDirectory;
    }

    public String getCreateNewFolder() {
        return createNewFolder;
    }

    public void setCreateNewFolder(String createNewFolder) {
        this.createNewFolder = createNewFolder;
    }

    public String getDateInFilename() {
        return dateInFilename;
    }

    public void setDateInFilename(String dateInFilename) {
        this.dateInFilename = dateInFilename;
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

    public String getFtpType() {
        return ftpType;
    }

    public void setFtpType(String ftpType) {
        this.ftpType = ftpType;
    }

    public String getControlEncoding() {
        return !"".equals(controlEncoding) ? FTPConstants.CONTROL_ENCODING_UTF : controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }
}
