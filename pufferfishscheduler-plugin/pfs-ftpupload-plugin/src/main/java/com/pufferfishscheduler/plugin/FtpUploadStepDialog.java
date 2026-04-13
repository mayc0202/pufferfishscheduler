package com.pufferfishscheduler.plugin;

import com.alibaba.fastjson2.JSONObject;
import com.pufferfishscheduler.plugin.common.Constants;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


public class FtpUploadStepDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = FtpUploadStepMeta.class; // for i18n purposes

    private FtpUploadStepMeta meta;
    // 表视图
    private TableView wFields;
    // ip
    private Text hostTxt;
    // 端口
    private Text portTxt;
    // 用户名
    private Text usernameTxt;
    // 密码
    private Text passwordTxt;
    // 工作模式
    private Text modeTxt;
    // 类型
    private Text typeTxt;
    // 本地目录
    private Text localDirectoryText;
    // 通配符
    private Text wildcardTxt;
    // 远程目录
    private Text remoteDirectoryTxt;
    // 输出字段配置
    private ColumnInfo[] outputFieldList;
    // 超时时间
    private Text timeoutTxt;
    // 二进制模式
    private Text binaryModeTxt;
    // 上传后删除本地文件
    private Text removeLocalFileTxt;
    // 不覆盖文件
    private Text overwriteFileTxt;

    public FtpUploadStepDialog(Shell parent, StepMetaInterface baseStepMeta, TransMeta transMeta, String stepname) {
        super(parent, baseStepMeta, transMeta, stepname);
        this.meta = (FtpUploadStepMeta) baseStepMeta;
    }


    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();
        this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.meta);
        ModifyListener lsMod = e -> FtpUploadStepDialog.this.meta.setChanged();
        this.changed = meta.hasChanged();
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        this.shell.setLayout(formLayout);
        this.shell.setText("FTP文件上传"); // 主键标题
        int middle = props.getMiddlePct();
        int margin = 4;

        /**
         * 步骤名称，配置label标签
         */
        this.wlStepname = new Label(this.shell, SWT.RIGHT);
        this.wlStepname.setText("步骤名称:");
        this.props.setLook(this.wlStepname);
        this.fdlStepname = new FormData();
        this.fdlStepname.left = new FormAttachment(0, 0);
        this.fdlStepname.right = new FormAttachment(middle, -margin);
        this.fdlStepname.top = new FormAttachment(0, margin);
        this.fdlStepname.bottom = new FormAttachment(0, 10 * margin);
        this.wlStepname.setLayoutData(this.fdlStepname);

        this.wStepname = new Text(this.shell, 18436);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.wStepname.addModifyListener(lsMod);
        this.fdStepname = new FormData();
        this.fdStepname.left = new FormAttachment(middle, 0);
        this.fdStepname.top = new FormAttachment(0, margin);
        this.fdStepname.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(this.fdStepname);

        Label hostLabel = new Label(shell, SWT.RIGHT);
        hostLabel.setText("ftp地址:");
        props.setLook(hostLabel);
        FormData fdHost = new FormData();
        fdHost.left = new FormAttachment(0, 0);
        fdHost.top = new FormAttachment(wlStepname, margin);
        fdHost.right = new FormAttachment(middle, -margin);
        hostLabel.setLayoutData(fdHost);

        hostTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(hostTxt);
        hostTxt.addModifyListener(lsMod);
        FormData fdHostText = new FormData();
        fdHostText.left = new FormAttachment(middle, 0);
        fdHostText.top = new FormAttachment(wStepname, 3 * margin);
        fdHostText.right = new FormAttachment(100, 0);
        hostTxt.setLayoutData(fdHostText);

        Label portLabel = new Label(shell, SWT.RIGHT);
        portLabel.setText("ftp端口:");
        props.setLook(portLabel);
        FormData fdPort = new FormData();
        fdPort.left = new FormAttachment(0, 0);
        fdPort.right = new FormAttachment(middle, -margin);
        fdPort.top = new FormAttachment(hostLabel, 2 * margin);
        portLabel.setLayoutData(fdPort);

        portTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(portTxt);
        portTxt.addModifyListener(lsMod);
        FormData fdPortText = new FormData();
        fdPortText.left = new FormAttachment(middle, 0);
        fdPortText.top = new FormAttachment(hostLabel, 2 * margin);
        fdPortText.right = new FormAttachment(100, 0);
        portTxt.setLayoutData(fdPortText);

        Label usernameLabel = new Label(shell, SWT.RIGHT);
        usernameLabel.setText("用户名:");
        props.setLook(usernameLabel);
        FormData fdUsername = new FormData();
        fdUsername.left = new FormAttachment(0, 0);
        fdUsername.right = new FormAttachment(middle, -margin);
        fdUsername.top = new FormAttachment(portLabel, 3 * margin);
        usernameLabel.setLayoutData(fdUsername);

        usernameTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(usernameTxt);
        usernameTxt.addModifyListener(lsMod);
        FormData fdUsernameText = new FormData();
        fdUsernameText.left = new FormAttachment(middle, 0);
        fdUsernameText.top = new FormAttachment(portLabel, 3 * margin);
        fdUsernameText.right = new FormAttachment(100, 0);
        usernameTxt.setLayoutData(fdUsernameText);

        Label pwdLabel = new Label(shell, SWT.RIGHT);
        pwdLabel.setText("密码:");
        props.setLook(pwdLabel);
        FormData fdPwd = new FormData();
        fdPwd.left = new FormAttachment(0, 0);
        fdPwd.right = new FormAttachment(middle, -margin);
        fdPwd.top = new FormAttachment(usernameLabel, 3 * margin);
        pwdLabel.setLayoutData(fdPwd);

        passwordTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(passwordTxt);
        passwordTxt.addModifyListener(lsMod);
        FormData fdPwdText = new FormData();
        fdPwdText.left = new FormAttachment(middle, 0);
        fdPwdText.top = new FormAttachment(usernameLabel, 3 * margin);
        fdPwdText.right = new FormAttachment(100, 0);
        passwordTxt.setLayoutData(fdPwdText);

        Label typeLabel = new Label(shell, SWT.RIGHT);
        typeLabel.setText("服务器类型:");
        props.setLook(typeLabel);
        FormData fdType = new FormData();
        fdType.left = new FormAttachment(0, 0);
        fdType.right = new FormAttachment(middle, -margin);
        fdType.top = new FormAttachment(pwdLabel, 3 * margin);
        typeLabel.setLayoutData(fdType);

        typeTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(typeTxt);
        typeTxt.addModifyListener(lsMod);
        FormData fdTypeText = new FormData();
        fdTypeText.left = new FormAttachment(middle, 0);
        fdTypeText.top = new FormAttachment(pwdLabel, 3 * margin);
        fdTypeText.right = new FormAttachment(100, 0);
        typeTxt.setLayoutData(fdTypeText);

        Label modeLabel = new Label(shell, SWT.RIGHT);
        modeLabel.setText("工作模式:");
        props.setLook(modeLabel);
        FormData fdMode = new FormData();
        fdMode.left = new FormAttachment(0, 0);
        fdMode.right = new FormAttachment(middle, -margin);
        fdMode.top = new FormAttachment(typeLabel, 3 * margin);
        modeLabel.setLayoutData(fdMode);

        modeTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(modeTxt);
        modeTxt.addModifyListener(lsMod);
        FormData fdModeText = new FormData();
        fdModeText.left = new FormAttachment(middle, 0);
        fdModeText.top = new FormAttachment(typeLabel, 3 * margin);
        fdModeText.right = new FormAttachment(100, 0);
        modeTxt.setLayoutData(fdModeText);

        Label wlLocalDirectoryLabel = new Label(shell, SWT.RIGHT);
        wlLocalDirectoryLabel.setText("本地目录:");
        props.setLook(wlLocalDirectoryLabel);
        FormData fdlLocalDirectory = new FormData();
        fdlLocalDirectory.left = new FormAttachment(0, 0);
        fdlLocalDirectory.right = new FormAttachment(middle, -margin);
        fdlLocalDirectory.top = new FormAttachment(modeLabel, 3 * margin);
        wlLocalDirectoryLabel.setLayoutData(fdlLocalDirectory);

        localDirectoryText = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(localDirectoryText);
        localDirectoryText.addModifyListener(lsMod);
        FormData fdlLocalDirectoryText = new FormData();
        fdlLocalDirectoryText.left = new FormAttachment(middle, 0);
        fdlLocalDirectoryText.top = new FormAttachment(modeLabel, 3 * margin);
        fdlLocalDirectoryText.right = new FormAttachment(100, 0);
        localDirectoryText.setLayoutData(fdlLocalDirectoryText);

        Label wildcardLabel = new Label(shell, SWT.RIGHT);
        wildcardLabel.setText("通配符（正则）:");
        props.setLook(wildcardLabel);
        FormData fdWildcard = new FormData();
        fdWildcard.left = new FormAttachment(0, 0);
        fdWildcard.right = new FormAttachment(middle, -margin);
        fdWildcard.top = new FormAttachment(wlLocalDirectoryLabel, 3 * margin);
        wildcardLabel.setLayoutData(fdWildcard);

        wildcardTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wildcardTxt);
        wildcardTxt.addModifyListener(lsMod);
        FormData fdWildcardText = new FormData();
        fdWildcardText.left = new FormAttachment(middle, 0);
        fdWildcardText.top = new FormAttachment(wlLocalDirectoryLabel, 3 * margin);
        fdWildcardText.right = new FormAttachment(100, 0);
        wildcardTxt.setLayoutData(fdWildcardText);

        Label remoteDirectoryLabel = new Label(shell, SWT.RIGHT);
        remoteDirectoryLabel.setText("远程目录:");
        props.setLook(remoteDirectoryLabel);
        FormData fdRemoteDirectory = new FormData();
        fdRemoteDirectory.left = new FormAttachment(0, 0);
        fdRemoteDirectory.right = new FormAttachment(middle, -margin);
        fdRemoteDirectory.top = new FormAttachment(wildcardLabel, 3 * margin);
        remoteDirectoryLabel.setLayoutData(fdRemoteDirectory);

        remoteDirectoryTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(remoteDirectoryTxt);
        remoteDirectoryTxt.addModifyListener(lsMod);
        FormData fdRemoteDirectoryText = new FormData();
        fdRemoteDirectoryText.left = new FormAttachment(middle, 0);
        fdRemoteDirectoryText.top = new FormAttachment(wildcardLabel, 3 * margin);
        fdRemoteDirectoryText.right = new FormAttachment(100, 0);
        remoteDirectoryTxt.setLayoutData(fdRemoteDirectoryText);

        Label timeoutLabel = new Label(shell, SWT.RIGHT);
        timeoutLabel.setText("超时时间:");
        props.setLook(timeoutLabel);
        FormData fdTimeout = new FormData();
        fdTimeout.left = new FormAttachment(0, 0);
        fdTimeout.right = new FormAttachment(middle, -margin);
        fdTimeout.top = new FormAttachment(remoteDirectoryLabel, 3 * margin);
        timeoutLabel.setLayoutData(fdTimeout);

        timeoutTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(timeoutTxt);
        timeoutTxt.addModifyListener(lsMod);
        FormData fdTimeoutText = new FormData();
        fdTimeoutText.left = new FormAttachment(middle, 0);
        fdTimeoutText.top = new FormAttachment(remoteDirectoryLabel, 3 * margin);
        fdTimeoutText.right = new FormAttachment(100, 0);
        timeoutTxt.setLayoutData(fdTimeoutText);

        Label binaryModeLabel = new Label(shell, SWT.RIGHT);
        binaryModeLabel.setText("二进制模式:");
        props.setLook(binaryModeLabel);
        FormData fdBinaryMode = new FormData();
        fdBinaryMode.left = new FormAttachment(0, 0);
        fdBinaryMode.right = new FormAttachment(middle, -margin);
        fdBinaryMode.top = new FormAttachment(timeoutLabel, 3 * margin);
        binaryModeLabel.setLayoutData(fdBinaryMode);

        binaryModeTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(binaryModeTxt);
        binaryModeTxt.addModifyListener(lsMod);
        FormData fdBinaryModeText = new FormData();
        fdBinaryModeText.left = new FormAttachment(middle, 0);
        fdBinaryModeText.top = new FormAttachment(timeoutLabel, 3 * margin);
        fdBinaryModeText.right = new FormAttachment(100, 0);
        binaryModeTxt.setLayoutData(fdBinaryModeText);

        Label removeLocalFileLabel = new Label(shell, SWT.RIGHT);
        removeLocalFileLabel.setText("上传后删除文件:");
        props.setLook(removeLocalFileLabel);
        FormData fdRemoveLocalFile = new FormData();
        fdRemoveLocalFile.left = new FormAttachment(0, 0);
        fdRemoveLocalFile.right = new FormAttachment(middle, -margin);
        fdRemoveLocalFile.top = new FormAttachment(binaryModeLabel, 3 * margin);
        removeLocalFileLabel.setLayoutData(fdRemoveLocalFile);

        removeLocalFileTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(removeLocalFileTxt);
        removeLocalFileTxt.addModifyListener(lsMod);
        FormData fdRemoveLocalFileText = new FormData();
        fdRemoveLocalFileText.left = new FormAttachment(middle, 0);
        fdRemoveLocalFileText.top = new FormAttachment(binaryModeLabel, 3 * margin);
        fdRemoveLocalFileText.right = new FormAttachment(100, 0);
        removeLocalFileTxt.setLayoutData(fdRemoveLocalFileText);

        Label overwriteFileLabel = new Label(shell, SWT.RIGHT);
        overwriteFileLabel.setText("不覆盖文件:");
        props.setLook(overwriteFileLabel);
        FormData fdOverwriteFile = new FormData();
        fdOverwriteFile.left = new FormAttachment(0, 0);
        fdOverwriteFile.right = new FormAttachment(middle, -margin);
        fdOverwriteFile.top = new FormAttachment(removeLocalFileLabel, 3 * margin);
        overwriteFileLabel.setLayoutData(fdOverwriteFile);

        overwriteFileTxt = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(overwriteFileTxt);
        overwriteFileTxt.addModifyListener(lsMod);
        FormData fdOverwriteFileText = new FormData();
        fdOverwriteFileText.left = new FormAttachment(middle, 0);
        fdOverwriteFileText.top = new FormAttachment(removeLocalFileLabel, 3 * margin);
        fdOverwriteFileText.right = new FormAttachment(100, 0);
        overwriteFileTxt.setLayoutData(fdOverwriteFileText);

        Label wlKey = new Label(this.shell, 0);
        wlKey.setText("输出字段");
        this.props.setLook(wlKey);
        FormData fdlKey = new FormData();
        fdlKey.left = new FormAttachment(0, 0);
        fdlKey.top = new FormAttachment(overwriteFileLabel, 3 * margin);
        wlKey.setLayoutData(fdlKey);
        int nrFieldCols = 2;
        int nrFieldRows = 5;
        this.outputFieldList = new ColumnInfo[nrFieldCols];
        this.outputFieldList[0] = new ColumnInfo("输出数据项目", 2, new String[]{""}, false);
        this.outputFieldList[1] = new ColumnInfo("输出字段名称", 2, new String[]{""}, false);
        this.wFields = new TableView(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER, this.outputFieldList, nrFieldRows, lsMod, this.props);
        FormData fdKey = new FormData();
        fdKey.left = new FormAttachment(0, 0);
        fdKey.top = new FormAttachment(wlKey, 2 * margin);
        fdKey.right = new FormAttachment(100, -margin);
        fdKey.bottom = new FormAttachment(100, -50);
        this.wFields.setLayoutData(fdKey);

        Runnable runnable = () -> {
            StepMeta stepMeta = FtpUploadStepDialog.this.transMeta.findStep(FtpUploadStepDialog.this.stepname);
            if (stepMeta != null) {
                FtpUploadStepDialog.this.setComboBoxes();
            }
        };

        (new Thread(runnable)).start();

        /**
         * 底部按钮 OK and cancel buttons
         */
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
        setButtonPositions(new Button[]{wOK, wCancel}, margin, null);
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOK = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };
        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);
        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };
        wStepname.addSelectionListener(lsDef);
        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });
        setSize();
        populateDialog();
        meta.setChanged(changed);
        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return stepname;
    }

    /**
     * 读取上一次数据
     */
    private void populateDialog() {
        wStepname.selectAll();
        String localDirectory = this.meta.getLocalDirectory();
        localDirectoryText.setText(localDirectory == null || localDirectory.equals("") ? "" : localDirectory);
        String remoteDirectory = this.meta.getRemoteDirectory();
        remoteDirectoryTxt.setText(remoteDirectory == null || remoteDirectory.equals("") ? "" : remoteDirectory);
        String wildcard = this.meta.getWildcard();
        wildcardTxt.setText(wildcard == null || wildcard.equals("") ? "" : wildcard);
        String host = this.meta.getHost();
        hostTxt.setText(host == null || host.equals("") ? "" : host);
        Long port = this.meta.getPort();
        portTxt.setText(port == 0L ? "" : port.toString());
        String username = String.valueOf(this.meta.getUsername());
        usernameTxt.setText(username == null || username.equals("null") ? "" : username);
        String password = String.valueOf(this.meta.getPassword());
        passwordTxt.setText(password == null || password.equals("null") ? "" : password);
        String type = String.valueOf(this.meta.getFtpType());
        typeTxt.setText(type == null || type.equals("null") ? "" : type);
        String mode = String.valueOf(this.meta.getMode());
        modeTxt.setText(mode == null || mode.equals("null") ? "" : mode);

        String timeout = String.valueOf(this.meta.getTimeout());
        timeoutTxt.setText(timeout.equals("0") ? "60000" : timeout);
        String binaryMode = String.valueOf(this.meta.getBinaryMode());
        binaryModeTxt.setText(binaryMode.equals("null") || binaryMode.equals("") ? "true" : binaryMode);
        String removeLocalFile = String.valueOf(this.meta.getRemoveLocalFile());
        removeLocalFileTxt.setText(removeLocalFile.equals("null") || removeLocalFile.equals("") ? "true" : removeLocalFile);
        String overwriteFile = String.valueOf(this.meta.getOverwriteFile());
        overwriteFileTxt.setText(overwriteFile.equals("null")|| overwriteFile.equals("") ? "false" : overwriteFile);

        String[] outputFieldList = this.meta.getOutputFieldList();
        for (int i = 0; i < outputFieldList.length; i++) {
            TableItem item = this.wFields.table.getItem(i);
            JSONObject obj = JSONObject.parseObject(outputFieldList[i]);
            if (obj != null && !"".equals(obj.toString())) {
                item.setText(1,obj.getString(Constants.PROJECT));
                item.setText(2,obj.getString(Constants.FIELD));
            }
        }
    }

    /**
     * 设置流中字段下拉框
     */
    protected void setComboBoxes() {
        this.outputFieldList[0].setComboValues(Constants.FIELD_NAMES);
    }

    /**
     * 取消
     */
    private void cancel() {
        this.stepname = null;
        this.meta.setChanged(changed);
        dispose();
    }

    /**
     * 确认
     */
    private void ok() {
        if (!Utils.isEmpty(this.wStepname.getText())) {
            this.getInfo(this.meta);
        }
        this.dispose();
    }

    /**
     * 获取信息，输出
     *
     * @param meta
     */
    private void getInfo(FtpUploadStepMeta meta) {
        //表视图不为空
        int nrkeys = this.wFields.nrNonEmpty();
        meta.allocate(nrkeys);
        this.meta.setHost(this.hostTxt.getText());
        String port = this.portTxt.getText();
        this.meta.setPort(null == port || "".equals(port) ? 0L : Long.parseLong(port));
        this.meta.setUsername(this.usernameTxt.getText());
        this.meta.setPassword(this.passwordTxt.getText());
        this.meta.setMode(this.modeTxt.getText());
        this.meta.setFtpType(this.typeTxt.getText());
        this.meta.setLocalDirectory(this.localDirectoryText.getText());
        this.meta.setRemoteDirectory(this.remoteDirectoryTxt.getText());
        this.meta.setWildcard(this.wildcardTxt.getText());
        String timeout = this.timeoutTxt.getText();
        this.meta.setTimeout(null == timeout || "".equals(timeout) ? 0L : Long.parseLong(timeout));
        this.meta.setBinaryMode(this.binaryModeTxt.getText());
        this.meta.setRemoveLocalFile(this.removeLocalFileTxt.getText());
        this.meta.setOverwriteFile(this.overwriteFileTxt.getText());
        String[] outputFields = new String[5];
        for (int i = 0; i < nrkeys; ++i) {
            TableItem item = this.wFields.getNonEmpty(i);
            JSONObject obj = new JSONObject();
            obj.put(Constants.PROJECT, item.getText(1));
            obj.put(Constants.FIELD, item.getText(2));
            outputFields[i] = obj.toJSONString();
        }
        this.meta.setOutputFieldList(outputFields);
        this.stepname = this.wStepname.getText();
    }

}
