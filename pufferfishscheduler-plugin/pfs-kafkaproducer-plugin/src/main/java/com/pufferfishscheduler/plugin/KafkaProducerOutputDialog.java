package com.pufferfishscheduler.plugin;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Kafka 输出步骤配置界面（直连 bootstrap）。
 */
public class KafkaProducerOutputDialog extends BaseStepDialog implements StepDialogInterface {

    private static final Class<?> PKG = KafkaProducerOutputMeta.class;
    private KafkaProducerOutputMeta meta;
    private TextVar wBootstrap;
    private TextVar wTopic;
    private TextVar wClientId;
    private TextVar wKeyField;
    private TextVar wMessageField;

    public KafkaProducerOutputDialog(Shell parent, StepMetaInterface baseStepMeta, TransMeta transMeta, String stepname) {
        super(parent, baseStepMeta, transMeta, stepname);
        this.meta = (KafkaProducerOutputMeta) baseStepMeta;
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();
        this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.meta);
        this.changed = this.meta.hasChanged();
        ModifyListener lsMod = e -> this.meta.setChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 15;
        formLayout.marginHeight = 15;
        this.shell.setLayout(formLayout);
        this.shell.setText("Kafka 输出");

        int middle = this.props.getMiddlePct();
        int margin = Const.MARGIN;

        this.wlStepname = new Label(this.shell, SWT.RIGHT);
        this.wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName", new String[0]));
        this.props.setLook(this.wlStepname);
        FormData fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.right = new FormAttachment(middle, -margin);
        fdlStepname.top = new FormAttachment(0, margin);
        this.wlStepname.setLayoutData(fdlStepname);
        this.wStepname = new org.eclipse.swt.widgets.Text(this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.wStepname.addModifyListener(lsMod);
        FormData fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(fdStepname);

        Control attach = this.wStepname;

        Label wlBootstrap = new Label(this.shell, SWT.RIGHT);
        wlBootstrap.setText("Bootstrap 地址：");
        this.props.setLook(wlBootstrap);
        FormData fdlBoot = new FormData();
        fdlBoot.left = new FormAttachment(0, 0);
        fdlBoot.right = new FormAttachment(middle, -margin);
        fdlBoot.top = new FormAttachment(attach, margin);
        wlBootstrap.setLayoutData(fdlBoot);
        this.wBootstrap = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wBootstrap);
        this.wBootstrap.addModifyListener(lsMod);
        FormData fdBoot = new FormData();
        fdBoot.left = new FormAttachment(middle, 0);
        fdBoot.top = new FormAttachment(attach, margin);
        fdBoot.right = new FormAttachment(100, 0);
        this.wBootstrap.setLayoutData(fdBoot);
        attach = this.wBootstrap.getTextWidget();

        Label wlTopic = new Label(this.shell, SWT.RIGHT);
        wlTopic.setText("Topic：");
        this.props.setLook(wlTopic);
        FormData fdlTopic = new FormData();
        fdlTopic.left = new FormAttachment(0, 0);
        fdlTopic.right = new FormAttachment(middle, -margin);
        fdlTopic.top = new FormAttachment(attach, margin);
        wlTopic.setLayoutData(fdlTopic);
        this.wTopic = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wTopic);
        this.wTopic.addModifyListener(lsMod);
        FormData fdTopic = new FormData();
        fdTopic.left = new FormAttachment(middle, 0);
        fdTopic.top = new FormAttachment(attach, margin);
        fdTopic.right = new FormAttachment(100, 0);
        this.wTopic.setLayoutData(fdTopic);
        attach = this.wTopic.getTextWidget();

        Label wlClient = new Label(this.shell, SWT.RIGHT);
        wlClient.setText("ClientId：");
        this.props.setLook(wlClient);
        FormData fdlClient = new FormData();
        fdlClient.left = new FormAttachment(0, 0);
        fdlClient.right = new FormAttachment(middle, -margin);
        fdlClient.top = new FormAttachment(attach, margin);
        wlClient.setLayoutData(fdlClient);
        this.wClientId = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wClientId);
        this.wClientId.addModifyListener(lsMod);
        FormData fdClient = new FormData();
        fdClient.left = new FormAttachment(middle, 0);
        fdClient.top = new FormAttachment(attach, margin);
        fdClient.right = new FormAttachment(100, 0);
        this.wClientId.setLayoutData(fdClient);
        attach = this.wClientId.getTextWidget();

        Label wlKey = new Label(this.shell, SWT.RIGHT);
        wlKey.setText("Key 字段名（可空）：");
        this.props.setLook(wlKey);
        FormData fdlKey = new FormData();
        fdlKey.left = new FormAttachment(0, 0);
        fdlKey.right = new FormAttachment(middle, -margin);
        fdlKey.top = new FormAttachment(attach, margin);
        wlKey.setLayoutData(fdlKey);
        this.wKeyField = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wKeyField);
        this.wKeyField.addModifyListener(lsMod);
        FormData fdKey = new FormData();
        fdKey.left = new FormAttachment(middle, 0);
        fdKey.top = new FormAttachment(attach, margin);
        fdKey.right = new FormAttachment(100, 0);
        this.wKeyField.setLayoutData(fdKey);
        attach = this.wKeyField.getTextWidget();

        Label wlMsg = new Label(this.shell, SWT.RIGHT);
        wlMsg.setText("Message 字段名：");
        this.props.setLook(wlMsg);
        FormData fdlMsg = new FormData();
        fdlMsg.left = new FormAttachment(0, 0);
        fdlMsg.right = new FormAttachment(middle, -margin);
        fdlMsg.top = new FormAttachment(attach, margin);
        wlMsg.setLayoutData(fdlMsg);
        this.wMessageField = new TextVar(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.wMessageField);
        this.wMessageField.addModifyListener(lsMod);
        FormData fdMsg = new FormData();
        fdMsg.left = new FormAttachment(middle, 0);
        fdMsg.top = new FormAttachment(attach, margin);
        fdMsg.right = new FormAttachment(100, 0);
        this.wMessageField.setLayoutData(fdMsg);

        populate();

        this.wOK = new Button(this.shell, SWT.PUSH);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));
        this.wCancel = new Button(this.shell, SWT.PUSH);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        setButtonPositions(new Button[]{this.wOK, this.wCancel}, margin, this.wMessageField.getTextWidget());

        this.lsCancel = (Event e) -> cancel();
        this.lsOK = (Event e) -> ok();
        this.wCancel.addListener(SWT.Selection, this.lsCancel);
        this.wOK.addListener(SWT.Selection, this.lsOK);
        this.lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);
        this.shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        setSize();
        this.meta.setChanged(this.changed);
        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return this.stepname;
    }

    private void populate() {
        this.wBootstrap.setText(Const.NVL(this.meta.getDirectBootstrapServers(), ""));
        this.wTopic.setText(Const.NVL(this.meta.getTopic(), ""));
        this.wClientId.setText(Const.NVL(this.meta.getClientId(), ""));
        this.wKeyField.setText(Const.NVL(this.meta.getKeyField(), ""));
        this.wMessageField.setText(Const.NVL(this.meta.getMessageField(), ""));
    }

    private void ok() {
        this.stepname = this.wStepname.getText();
        this.meta.setDirectBootstrapServers(this.wBootstrap.getText());
        this.meta.setTopic(this.wTopic.getText());
        this.meta.setClientId(this.wClientId.getText());
        this.meta.setKeyField(this.wKeyField.getText());
        this.meta.setMessageField(this.wMessageField.getText());
        this.meta.setChanged();
        dispose();
    }

    private void cancel() {
        this.stepname = null;
        this.meta.setChanged(this.changed);
        dispose();
    }
}
