package com.pufferfishscheduler.plugin;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONReader;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * DebeziumJsonStepDialog
 */
public class DebeziumJsonStepDialog extends BaseStepDialog implements StepDialogInterface {
    private static Class<?> PKG = DebeziumJsonStepMeta.class;
    private DebeziumJsonStepMeta meta;
    private Text jsonText;
    private Text inputFieldTxt;
    private Text msgStructureTxt;
    private Button wHeader;

    public DebeziumJsonStepDialog(Shell parent, StepMetaInterface baseStepMeta, TransMeta transMeta, String stepname) {
        super(parent, baseStepMeta, transMeta, stepname);
        this.meta = (DebeziumJsonStepMeta) baseStepMeta;
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();
        this.shell = new Shell(parent, 3312);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.meta);
        ModifyListener lsMod = e -> {
            this.meta.setChanged();
        };
        this.changed = this.meta.hasChanged();
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        this.shell.setLayout(formLayout);
        this.shell.setText("解析Debezium格式Json报文");
        int middle = this.props.getMiddlePct();
        this.wlStepname = new Label(this.shell, 131072);
        this.wlStepname.setText("步骤名称：");
        this.props.setLook(this.wlStepname);
        this.fdlStepname = new FormData();
        this.fdlStepname.left = new FormAttachment(0, 0);
        this.fdlStepname.right = new FormAttachment(middle, -4);
        this.fdlStepname.top = new FormAttachment(0, 4);
        this.fdlStepname.bottom = new FormAttachment(0, 10 * 4);
        this.wlStepname.setLayoutData(this.fdlStepname);
        this.wStepname = new Text(this.shell, 18436);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.wStepname.addModifyListener(lsMod);
        this.fdStepname = new FormData();
        this.fdStepname.left = new FormAttachment(middle, 0);
        this.fdStepname.top = new FormAttachment(0, 4);
        this.fdStepname.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(this.fdStepname);
        Label wlHello = new Label(this.shell, 131072);
        wlHello.setText("样例JSON报文：");
        this.props.setLook(wlHello);
        FormData fdlHello = new FormData();
        fdlHello.left = new FormAttachment(0, 0);
        fdlHello.right = new FormAttachment(middle, -4);
        fdlHello.top = new FormAttachment(this.wlStepname, 4);
        wlHello.setLayoutData(fdlHello);
        this.jsonText = new Text(this.shell, 18436);
        this.props.setLook(this.jsonText);
        this.jsonText.addModifyListener(lsMod);
        FormData fdHelloText = new FormData();
        fdHelloText.left = new FormAttachment(middle, 0);
        fdHelloText.top = new FormAttachment(this.wStepname, 3 * 4);
        fdHelloText.right = new FormAttachment(100, 0);
        this.jsonText.setLayoutData(fdHelloText);
        Label inputLabel = new Label(this.shell, 131072);
        inputLabel.setText("输入流字段名（需要解构的报文字段名）：");
        this.props.setLook(inputLabel);
        FormData fdInput = new FormData();
        fdInput.left = new FormAttachment(0, 0);
        fdInput.right = new FormAttachment(middle, -4);
        fdInput.top = new FormAttachment(wlHello, 3 * 4);
        inputLabel.setLayoutData(fdInput);
        this.inputFieldTxt = new Text(this.shell, 18436);
        this.props.setLook(this.inputFieldTxt);
        this.inputFieldTxt.addModifyListener(lsMod);
        FormData fdInputText = new FormData();
        fdInputText.left = new FormAttachment(middle, 0);
        fdInputText.top = new FormAttachment(wlHello, 3 * 4);
        fdInputText.right = new FormAttachment(100, 0);
        this.inputFieldTxt.setLayoutData(fdInputText);
        Label msgStructureLabel = new Label(this.shell, 131072);
        msgStructureLabel.setText("解构后字段配置：");
        this.props.setLook(msgStructureLabel);
        FormData fdMsgStructure = new FormData();
        fdMsgStructure.left = new FormAttachment(0, 0);
        fdMsgStructure.right = new FormAttachment(middle, -4);
        fdMsgStructure.top = new FormAttachment(inputLabel, 3 * 4);
        msgStructureLabel.setLayoutData(fdMsgStructure);
        this.msgStructureTxt = new Text(this.shell, 18436);
        this.props.setLook(this.msgStructureTxt);
        this.msgStructureTxt.addModifyListener(lsMod);
        FormData fdMsgStructureText = new FormData();
        fdMsgStructureText.left = new FormAttachment(middle, 0);
        fdMsgStructureText.top = new FormAttachment(inputLabel, 3 * 4);
        fdMsgStructureText.right = new FormAttachment(100, 0);
        this.msgStructureTxt.setLayoutData(fdMsgStructureText);
        Label wlHeader = new Label(this.shell, 131072);
        wlHeader.setText("解构后舍弃Debezium JSON报文：");
        this.props.setLook(wlHeader);
        FormData fdlHeader = new FormData();
        fdlHeader.left = new FormAttachment(0, 0);
        fdlHeader.right = new FormAttachment(middle, -4);
        fdlHeader.top = new FormAttachment(msgStructureLabel, 3 * 4);
        wlHeader.setLayoutData(fdlHeader);
        this.wHeader = new Button(this.shell, 32);
        this.props.setLook(this.wHeader);
        FormData fdHeader = new FormData();
        fdHeader.left = new FormAttachment(middle, 0);
        fdHeader.top = new FormAttachment(msgStructureLabel, 3 * 4);
        fdHeader.right = new FormAttachment(100, 0);
        this.wHeader.setLayoutData(fdHeader);
        this.wHeader.setSelection(false);
        this.wHeader.addSelectionListener(new SelectionAdapter() { // from class: com.lsd.dtdesigner.plugin.debeziumjson.DebeziumJsonStepDialog.1
            public void widgetSelected(SelectionEvent arg0) {
                DebeziumJsonStepDialog.this.changed = true;
            }
        });
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));
        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        setButtonPositions(new Button[]{this.wOK, this.wCancel}, 4, null);
        this.lsCancel = new Listener() { // from class: com.lsd.dtdesigner.plugin.debeziumjson.DebeziumJsonStepDialog.2
            public void handleEvent(Event e2) {
                DebeziumJsonStepDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() { // from class: com.lsd.dtdesigner.plugin.debeziumjson.DebeziumJsonStepDialog.3
            public void handleEvent(Event e2) {
                DebeziumJsonStepDialog.this.ok();
            }
        };
        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);
        this.lsDef = new SelectionAdapter() { // from class: com.lsd.dtdesigner.plugin.debeziumjson.DebeziumJsonStepDialog.4
            public void widgetDefaultSelected(SelectionEvent e2) {
                DebeziumJsonStepDialog.this.ok();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);
        this.shell.addShellListener(new ShellAdapter() { // from class: com.lsd.dtdesigner.plugin.debeziumjson.DebeziumJsonStepDialog.5
            public void shellClosed(ShellEvent e2) {
                DebeziumJsonStepDialog.this.cancel();
            }
        });
        setSize();
        populateDialog();
        this.meta.setChanged(this.changed);
        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return this.stepname;
    }

    private void populateDialog() {
        this.wStepname.selectAll();
        this.jsonText.setText(this.meta.getSampleData());
        this.inputFieldTxt.setText(this.meta.getSourceField());
        this.msgStructureTxt.setText(this.meta.getMessageStructure());
        this.wHeader.setSelection(false);
    }

    /* JADX INFO: Access modifiers changed from: private */
    public void cancel() {
        this.stepname = null;
        this.meta.setChanged(this.changed);
        dispose();
    }

    /* JADX INFO: Access modifiers changed from: private */
    public void ok() {
        this.stepname = this.wStepname.getText();
        this.meta.setSampleData(this.jsonText.getText());
        this.meta.setSourceField(this.inputFieldTxt.getText());
        this.meta.setMessageStructure(this.msgStructureTxt.getText());
        String text = this.msgStructureTxt.getText();
        if (null != text && !"".equals(text)) {
            JSONArray jsonArray = JSONArray.parseArray(text, new JSONReader.Feature[0]);
            String[] config = new String[jsonArray.size()];
            for (int i = 0; i < jsonArray.size(); i++) {
                config[i] = jsonArray.getString(i);
            }
            this.meta.setOutputFieldConfig(config);
        }
        dispose();
    }
}
