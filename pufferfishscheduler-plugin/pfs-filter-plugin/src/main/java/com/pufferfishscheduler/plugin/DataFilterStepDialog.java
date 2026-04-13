package com.pufferfishscheduler.plugin;

import org.eclipse.swt.events.ModifyEvent;
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
 * 数据过滤步骤对话框
 */
public class DataFilterStepDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = DataFilterStepMeta.class;
    private DataFilterStepMeta meta;
    private Text wHelloText;

    public DataFilterStepDialog(Shell parent, StepMetaInterface baseStepMeta, TransMeta transMeta, String stepname) {
        super(parent, baseStepMeta, transMeta, stepname);
        this.meta = (DataFilterStepMeta) baseStepMeta;
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();
        this.shell = new Shell(parent, 3312);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.meta);
        this.changed = this.meta.hasChanged();
        ModifyListener lsMod = new ModifyListener() { // from class: com.lsd.dtdesigner.plugin.filter.FilterStepDialog.1
            public void modifyText(ModifyEvent e) {
                DataFilterStepDialog.this.meta.setChanged();
            }
        };
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        this.shell.setLayout(formLayout);
        this.shell.setText("Demo演示插件");
        int middle = this.props.getMiddlePct();
        this.wlStepname = new Label(this.shell, 131072);
        this.wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName", new String[0]));
        this.props.setLook(this.wlStepname);
        this.fdlStepname = new FormData();
        this.fdlStepname.left = new FormAttachment(0, 0);
        this.fdlStepname.right = new FormAttachment(middle, -4);
        this.fdlStepname.top = new FormAttachment(0, 4);
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
        wlHello.setText("hello");
        this.props.setLook(wlHello);
        FormData fdlHello = new FormData();
        fdlHello.left = new FormAttachment(0, 0);
        fdlHello.right = new FormAttachment(middle, -4);
        fdlHello.top = new FormAttachment(this.wlStepname, 4);
        wlHello.setLayoutData(fdlHello);
        this.wHelloText = new Text(this.shell, 18436);
        this.props.setLook(this.wHelloText);
        this.wHelloText.addModifyListener(lsMod);
        FormData fdHelloText = new FormData();
        fdHelloText.left = new FormAttachment(middle, 0);
        fdHelloText.top = new FormAttachment(this.wStepname, 4);
        fdHelloText.right = new FormAttachment(100, 0);
        this.wHelloText.setLayoutData(fdHelloText);
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));
        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        setButtonPositions(new Button[]{this.wOK, this.wCancel}, 4, null);
        this.lsCancel = new Listener() {
            public void handleEvent(Event e) {
                DataFilterStepDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() {
            public void handleEvent(Event e) {
                DataFilterStepDialog.this.ok();
            }
        };
        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);
        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                DataFilterStepDialog.this.ok();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);
        this.shell.addShellListener(new ShellAdapter() { // from class: com.lsd.dtdesigner.plugin.filter.FilterStepDialog.5
            public void shellClosed(ShellEvent e) {
                DataFilterStepDialog.this.cancel();
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
        this.wHelloText.setText("");
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
        dispose();
    }
}
