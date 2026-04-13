package com.pufferfishscheduler.plugin;

import com.pufferfishscheduler.plugin.common.Constants;
import com.pufferfishscheduler.plugin.common.enums.CleanType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataCleanStepDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = DataCleanStepMeta.class; // for i18n purposes

    // 表视图
    private TableView wFields;
    // 清洗组件步骤数据
    private DataCleanStepMeta meta;
    // 输入字段
    private Map<String, Integer> inputFields;

    // addres
    FormData addressForm;
    Text addressText;
    FormData addressFormText;

    // apiKey
    FormData apiKeyForm;
    Text apiKeyText;
    FormData apiKeyFormText;

    // 列信息
    private ColumnInfo[] cikey;
    // 数据类型
    static List<String> dataType = new ArrayList<>();
    // 清洗方式
    CleanType[] cleanTypes;

    static {
        dataType.add("Timestamp");
        dataType.add("String");
        dataType.add("Number");
        dataType.add("Internet Address");
        dataType.add("Integer");
        dataType.add("Date");
        dataType.add("Boolean");
        dataType.add("Binary");
        dataType.add("BigNumber");
    }

    // 缓存流中字段
    private List<Map<String, Object>> flowFields;


    public DataCleanStepDialog(Shell parent, StepMetaInterface baseStepMeta, TransMeta transMeta, String stepname) {
        super(parent, baseStepMeta, transMeta, stepname);
        this.meta = (DataCleanStepMeta) baseStepMeta;
        this.inputFields = new HashMap<>();
    }

    @Override
    public String open() {
        // 存储一些方便的SWT变量
        Shell parent = getParent();
        Display display = parent.getDisplay();

        // 用于准备对话框的SWT代码
        // shell是程序主窗口
        this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.meta);

        // 在所有控件上使用的ModifyListener。它将元对象更新为表示正在进行更改。
        ModifyListener lsMod = e -> DataCleanStepDialog.this.meta.setChanged();

        // 保存元对象上已更改标志的值。如果用户取消
        // 对话框中，它将恢复为此保存值。
        // “已更改”变量继承自BaseStepDialog
        this.changed = meta.hasChanged();

        // 用于构建实际设置对话框的SWT代码
        FormLayout formLayout = new FormLayout();
        // 外边距宽度
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        // 设置布局
        this.shell.setLayout(formLayout);
        this.shell.setText("数据清洗组件"); // 主键标题
        // 获取中间百分比
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

        Label wlAddress = new Label(this.shell, SWT.RIGHT);
        wlAddress.setText("API地址：");
        this.props.setLook(wlAddress);
        this.addressForm = new FormData();
        this.addressForm.left = new FormAttachment(0, 0);
        this.addressForm.right = new FormAttachment(middle, -margin);
        this.addressForm.top = new FormAttachment(wStepname, 5 * margin);
        wlAddress.setLayoutData(this.addressForm);
        this.addressText = new Text(this.shell, 18436);
        this.props.setLook(this.addressText);
        this.addressText.addModifyListener(lsMod);
        this.addressFormText = new FormData();
        this.addressFormText.left = new FormAttachment(middle, 0);
        this.addressFormText.top = new FormAttachment(wStepname, 5 * margin);
        this.addressFormText.right = new FormAttachment(100, 0);
        this.addressText.setLayoutData(this.addressFormText);

        Label wlApiKey = new Label(this.shell, SWT.RIGHT);
        wlApiKey.setText("API-KEY：");
        this.props.setLook(wlApiKey);
        this.apiKeyForm = new FormData();
        this.apiKeyForm.left = new FormAttachment(0, 0);
        this.apiKeyForm.right = new FormAttachment(middle, -margin);
        this.apiKeyForm.top = new FormAttachment(addressText, 5 * margin);
        wlApiKey.setLayoutData(this.apiKeyForm);
        this.apiKeyText = new Text(this.shell, 18436);
        this.props.setLook(this.apiKeyText);
        this.apiKeyText.addModifyListener(lsMod);
        this.apiKeyFormText = new FormData();
        this.apiKeyFormText.left = new FormAttachment(middle, 0);
        this.apiKeyFormText.top = new FormAttachment(addressText, 5 * margin);
        this.apiKeyFormText.right = new FormAttachment(100, 0);
        this.apiKeyText.setLayoutData(this.apiKeyFormText);


        /**
         * 选择清洗字段
         */
        Label wlKey = new Label(this.shell, 0);
        wlKey.setText("请选择需要清洗转换的字段");
        this.props.setLook(wlKey);
        FormData fdlKey = new FormData();
        fdlKey.left = new FormAttachment(0, 0);
        fdlKey.top = new FormAttachment(apiKeyText, 3 * margin);
        wlKey.setLayoutData(fdlKey);
        // 列
//        int nrFieldCols = 9;
        int nrFieldCols = 5;
        // 行
        int nrFieldRows = this.meta.getFieldName() != null ? this.meta.getFieldName().length : 1;
        this.cikey = new ColumnInfo[nrFieldCols];
        this.cikey[0] = new ColumnInfo("字段名称", 2, new String[]{""}, false);
        this.cikey[1] = new ColumnInfo("参数", 1, false);
        this.cikey[2] = new ColumnInfo("新字段", 1, false);
        this.cikey[3] = new ColumnInfo("新字段数据类型", 2, new String[]{""}, false);
        this.cikey[4] = new ColumnInfo("规则Id", 1, false);
        //文本输入框字段，窗口部件
        this.wFields = new TableView(this.transMeta, this.shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER, this.cikey, nrFieldRows, lsMod, this.props);
        //表单数据的键值对 key/value 的构造方式
        FormData fdKey = new FormData();
        //“步骤名称”文本输入框控件布局信息
        fdKey.left = new FormAttachment(0, 0);
        fdKey.top = new FormAttachment(wlKey, 2 * margin);
        fdKey.right = new FormAttachment(100, -margin);
        fdKey.bottom = new FormAttachment(100, -50);
        //设置布局内容
        this.wFields.setLayoutData(fdKey);
        Runnable runnable = () -> {
            StepMeta stepMeta = DataCleanStepDialog.this.transMeta.findStep(DataCleanStepDialog.this.stepname);
            if (stepMeta != null) {
                try {
                    RowMetaInterface row = DataCleanStepDialog.this.transMeta.getPrevStepFields(stepMeta);
                    // 处理流中字段
                    dealFlowFields(row.getValueMetaList());
                    for (int i = 0; i < row.size(); ++i) {
                        DataCleanStepDialog.this.inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                    }
                    // 获取流中字段
                    DataCleanStepDialog.this.setComboBoxes();
                } catch (KettleException var4) {
                    DataCleanStepDialog.this.logError("It was not possible to get the fields from the previous step(s).");
                }
            }

        };

        (new Thread(runnable)).start();

        /**
         * 底部按钮 OK and cancel buttons
         */
        this.wOK = new Button(this.shell, SWT.PUSH);
        this.wOK.setText("确定");
        this.wCancel = new Button(this.shell, SWT.PUSH);
        this.wCancel.setText("取消");
        setButtonPositions(new Button[]{this.wOK, this.wCancel}, margin, null);

        // 给按钮添加监听器
        this.lsOK = e -> DataCleanStepDialog.this.ok();
        this.wOK.addListener(13, this.lsOK);

        this.lsCancel = e -> DataCleanStepDialog.this.cancel();
        this.wCancel.addListener(13, this.lsCancel);

        // 默认侦听器（用于点击“回车”）
        this.lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                DataCleanStepDialog.this.ok();
            }
        };
        //添加选择侦听器( lsDef );
        this.wStepname.addSelectionListener(this.lsDef);

        // 检测X或ALT-F4或其他会关闭此窗口的东西，并正确取消对话框
        this.shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent e) {
                DataCleanStepDialog.this.cancel();
            }
        });

        // 根据屏幕上的最后位置设置/恢复对话框大小
        this.setSize();

        // 获取数据
        this.getData();

        // 当对话框填充期间修改侦听器启动时，将更改的标志恢复为原始值
        this.meta.setChanged(this.changed);

        // 打开对话框并进入事件循环
        this.shell.open();
        // 如果没有处理，就不显示
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        // 此时，对话框已关闭，因此已执行ok（）或cancel（）
        // “stepname”变量继承自BaseStepDialog
        return this.stepname;
    }


    /**
     * 处理流中字段
     *
     * @param list
     */
    private void dealFlowFields(List<ValueMetaInterface> list) {
        if (list.size() > 0) {
            this.flowFields = new ArrayList<>();
            list.forEach(s -> {
                Map<String, Object> field = new HashMap<>();
                field.put(Constants.FIELD_NAME, s.getName());
                field.put(Constants.FIELD_TYPE, s.getType());
                flowFields.add(field);
            });
        }
    }

    /**
     * 设置流中字段下拉框
     */
    protected void setComboBoxes() {
        // 处理流中字段
        if (null != this.flowFields) {
            String[] fieldNames = new String[this.flowFields.size()];
            for (int i = 0; i < fieldNames.length; i++) {
                fieldNames[i] = this.flowFields.get(i).get(Constants.FIELD_NAME).toString();
            }
            // 处理数据类型
            String[] flowDataType = new String[dataType.size()];
            for (int i = 0; i < flowDataType.length; i++) {
                flowDataType[i] = dataType.get(i);
            }
            // 视图列表绑定下拉属性
            this.cikey[0].setComboValues(fieldNames);
            this.cikey[3].setComboValues(flowDataType);
        }
    }


    /**
     * 获取数据
     */
    public void getData() {
        this.addressText.setText(this.meta.getAddress());
        this.apiKeyText.setText(this.meta.getApiKey());
        if (null != this.meta.getFieldName() && this.meta.getFieldName().length > 0) {
            for (int i = 0; i < this.meta.getFieldName().length; ++i) {
                TableItem item = this.wFields.table.getItem(i);
                if (this.meta.getFieldName()[i] != null) {
                    item.setText(1, this.meta.getFieldName()[i]);
                }
                if (this.meta.getParams()[i] != null) {
                    item.setText(2, this.meta.getParams()[i]);
                }
                if (this.meta.getRename()[i] != null) {
                    item.setText(3, this.meta.getRename()[i]);
                }
                if (this.meta.getRenameType()[i] != null) {
                    item.setText(4, this.meta.getRenameType()[i]);
                }
                if (this.meta.getRuleId()[i] != null) {
                    item.setText(5, this.meta.getRuleId()[i]);
                }
            }
        }
        this.wFields.setRowNums();
        this.wFields.optWidth(true);
        this.wStepname.selectAll();
        this.wStepname.setFocus();
    }

    /**
     * 当用户取消对话框时调用。
     */
    private void cancel() {
        // The "stepname" variable will be the return value for the open() method.
        // Setting to null to indicate that dialog was cancelled.
        this.stepname = null;
        // 正在恢复met-aobject上的原始“已更改”标志
        // 关闭弹窗
        dispose();
    }

    /**
     * 当用户确认对话框时调用
     * todo这个地方有可能根据算法会有响应的要求
     */
    private void ok() {
        if (!Utils.isEmpty(this.wStepname.getText())) {
            this.getInfo(this.meta);
            DataCleanStepMeta meta = this.meta;
        }
        //关闭SWT对话框窗口
        this.dispose();
    }

    /**
     * 获取信息，输出
     *
     * @param meta
     */
    private void getInfo(DataCleanStepMeta meta) {
        //表视图不为空
        int nrkeys = this.wFields.nrNonEmpty();
        meta.allocate(nrkeys);
        if (this.log.isDebug()) {
            this.logDebug("清洗基础字段", new String[]{String.valueOf(nrkeys)});
        }
        //存值进去
        this.meta.setApiKey(this.apiKeyText.getText());
        this.meta.setAddress(this.addressText.getText());
        for (int i = 0; i < nrkeys; ++i) {
            //获取非空的
            TableItem item = this.wFields.getNonEmpty(i);
            meta.getFieldName()[i] = item.getText(1);
            meta.getParams()[i] = item.getText(2);
            meta.getRename()[i] = item.getText(3);
            meta.getRenameType()[i] = item.getText(4);
            meta.getRuleId()[i] = item.getText(5);
        }
        this.stepname = this.wStepname.getText();
    }
}
