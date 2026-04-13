package com.pufferfishscheduler.plugin;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStreamingDialog;

/* JADX INFO: loaded from: lsd-bigdata-plugin-2.3.2.jar:com/lsd/dtdesigner/plugin/bigdata/kafka/KafkaConsumerInputDialog.class */
public class KafkaConsumerInputDialog extends BaseStreamingDialog implements StepDialogInterface {
    private static final int INPUT_WIDTH = 350;
    private static final int SHELL_MIN_WIDTH = 527;
    private static final int SHELL_MIN_HEIGHT = 682;
    private static final Class<?> PKG = KafkaConsumerInputMeta.class;
    private static final ImmutableMap<String, String> DEFAULT_OPTION_VALUES = ImmutableMap.of("auto.offset.reset", "latest");
    private final KafkaFactory kafkaFactory;
    private KafkaConsumerInputMeta consumerMeta;
    private Spoon spoonInstance;
    private Label wlClusterName;
    private ComboVar wClusterName;
    private TextVar wConsumerGroup;
    private TableView topicsTable;
    private TableView optionsTable;
    private Button wbDirect;
    private Button wbCluster;
    private Label wlBootstrapServers;
    private TextVar wBootstrapServers;
    private Button wbAutoCommit;
    private Button wbManualCommit;
    private static final String REPOS_DELIM = "/";
    public static final String L_DIALOGTITLE = "Kafka输入";
    public static final String L_STEPNAME = "步骤名称：";
    public static final String L_TRANSPATH = "处理消息转换：";
    public static final String L_CONNECT_CONFIG = "连接配置";
    public static final String L_BATCH_CONFIG = "批次配置";
    public static final String L_BATCH_DURATION = "每次获取消息持续时间（毫秒）：";
    public static final String L_BATCH_BATCHSIZE = "每次最大获取消息数量：";
    public static final String L_BATCH_PARALLELISM = "最大同时生成的批次数量：";
    public static final String L_BATCH_PREFETCHCOUNT = "最大预取记录数：";

    public KafkaConsumerInputDialog(Shell parent, Object in, TransMeta tr, String sname) {
        super(parent, in, tr, sname);
        this.kafkaFactory = KafkaFactory.defaultFactory();
        this.consumerMeta = (KafkaConsumerInputMeta) in;
        this.spoonInstance = Spoon.getInstance();
    }

    protected String getDialogTitle() {
        return L_DIALOGTITLE;
    }

    protected void createAdditionalTabs() {
        this.shell.setMinimumSize(SHELL_MIN_WIDTH, SHELL_MIN_HEIGHT);
        buildFieldsTab();
        buildOptionsTab();
        buildOffsetManagement();
        this.wlStepname.setText("步骤名称：");
        this.wlTransPath.setText(L_TRANSPATH);
        this.wSetupTab.setText("连接配置");
        this.wBatchTab.setText(L_BATCH_CONFIG);
        this.wlBatchDuration.setText(L_BATCH_DURATION);
        this.wlBatchSize.setText(L_BATCH_BATCHSIZE);
        this.wResultsTab.setText("输出结果字段配置");
        this.wlSubStep.setText("从消息处理流程中选择返回结果的步骤：");
    }

    private void buildOffsetManagement() {
        Group wOffsetGroup = new Group(this.wBatchComp, 16);
        wOffsetGroup.setText("偏移量提交方式");
        FormLayout flOffsetGroup = new FormLayout();
        flOffsetGroup.marginHeight = 15;
        flOffsetGroup.marginWidth = 15;
        wOffsetGroup.setLayout(flOffsetGroup);
        FormData fdOffsetGroup = new FormData();
        fdOffsetGroup.top = new FormAttachment(this.wPrefetchCount, 15);
        fdOffsetGroup.left = new FormAttachment(0, 0);
        fdOffsetGroup.right = new FormAttachment(100, 0);
        wOffsetGroup.setLayoutData(fdOffsetGroup);
        this.props.setLook(wOffsetGroup);
        this.wbAutoCommit = new Button(wOffsetGroup, 16);
        this.wbAutoCommit.setText("读取消息后立即提交");
        FormData fdbAutoCommit = new FormData();
        fdbAutoCommit.top = new FormAttachment(0, 0);
        fdbAutoCommit.left = new FormAttachment(0, 0);
        this.wbAutoCommit.setLayoutData(fdbAutoCommit);
        this.props.setLook(this.wbAutoCommit);
        this.wbManualCommit = new Button(wOffsetGroup, 16);
        this.wbManualCommit.setText("消息处理流程执行成功后提交");
        FormData fdbManualCommit = new FormData();
        fdbManualCommit.left = new FormAttachment(0, 0);
        fdbManualCommit.top = new FormAttachment(this.wbAutoCommit, 10, 1024);
        this.wbManualCommit.setLayoutData(fdbManualCommit);
        this.props.setLook(this.wbManualCommit);
    }

    protected void buildSetup(Composite wSetupComp) {
        this.props.setLook(wSetupComp);
        FormLayout setupLayout = new FormLayout();
        setupLayout.marginHeight = 15;
        setupLayout.marginWidth = 15;
        wSetupComp.setLayout(setupLayout);
        Group wConnectionGroup = new Group(wSetupComp, 16);
        wConnectionGroup.setText("连接方式");
        FormLayout flConnection = new FormLayout();
        flConnection.marginHeight = 15;
        flConnection.marginWidth = 15;
        wConnectionGroup.setLayout(flConnection);
        FormData fdConnectionGroup = new FormData();
        fdConnectionGroup.left = new FormAttachment(0, 0);
        fdConnectionGroup.top = new FormAttachment(0, 0);
        fdConnectionGroup.right = new FormAttachment(100, 0);
        fdConnectionGroup.width = INPUT_WIDTH;
        wConnectionGroup.setLayoutData(fdConnectionGroup);
        this.props.setLook(wConnectionGroup);
        this.wbDirect = new Button(wConnectionGroup, 16);
        this.wbDirect.setText("直连");
        FormData fdbDirect = new FormData();
        fdbDirect.left = new FormAttachment(0, 0);
        fdbDirect.top = new FormAttachment(0, 0);
        this.wbDirect.setLayoutData(fdbDirect);
        this.wbDirect.addSelectionListener(new SelectionListener() { // from class: com.lsd.dtdesigner.plugin.bigdata.kafka.KafkaConsumerInputDialog.1
            public void widgetSelected(SelectionEvent selectionEvent) {
                KafkaConsumerInputDialog.this.lsMod.modifyText((ModifyEvent) null);
                KafkaConsumerInputDialog.this.toggleVisibility(true);
            }

            public void widgetDefaultSelected(SelectionEvent selectionEvent) {
                KafkaConsumerInputDialog.this.toggleVisibility(true);
            }
        });
        this.props.setLook(this.wbDirect);
        this.wbCluster = new Button(wConnectionGroup, 16);
        this.wbCluster.setText("集群");
        FormData fdbCluster = new FormData();
        fdbCluster.left = new FormAttachment(0, 0);
        fdbCluster.top = new FormAttachment(this.wbDirect, 10);
        this.wbCluster.setLayoutData(fdbCluster);
        this.wbCluster.addSelectionListener(new SelectionListener() { // from class: com.lsd.dtdesigner.plugin.bigdata.kafka.KafkaConsumerInputDialog.2
            public void widgetSelected(SelectionEvent selectionEvent) {
                KafkaConsumerInputDialog.this.lsMod.modifyText((ModifyEvent) null);
                KafkaConsumerInputDialog.this.toggleVisibility(false);
            }

            public void widgetDefaultSelected(SelectionEvent selectionEvent) {
                KafkaConsumerInputDialog.this.toggleVisibility(false);
            }
        });
        this.props.setLook(this.wbCluster);
        Label environmentSeparator = new Label(wConnectionGroup, 514);
        FormData fdenvironmentSeparator = new FormData();
        fdenvironmentSeparator.top = new FormAttachment(this.wbDirect, 0, 128);
        fdenvironmentSeparator.left = new FormAttachment(this.wbCluster, 15);
        fdenvironmentSeparator.bottom = new FormAttachment(this.wbCluster, 0, 1024);
        environmentSeparator.setLayoutData(fdenvironmentSeparator);
        this.wlClusterName = new Label(wConnectionGroup, 16384);
        this.props.setLook(this.wlClusterName);
        this.wlClusterName.setText("Hadoop集群地址：");
        FormData fdlClusterName = new FormData();
        fdlClusterName.left = new FormAttachment(environmentSeparator, 15);
        fdlClusterName.top = new FormAttachment(0, 0);
        fdlClusterName.right = new FormAttachment(78, 0);
        this.wlClusterName.setLayoutData(fdlClusterName);
        this.wClusterName = new ComboVar(this.transMeta, wConnectionGroup, 18436);
        this.props.setLook(this.wClusterName);
        this.wClusterName.addModifyListener(this.lsMod);
        FormData fdClusterName = new FormData();
        fdClusterName.left = new FormAttachment(this.wlClusterName, 0, 16384);
        fdClusterName.top = new FormAttachment(this.wlClusterName, 5);
        fdClusterName.right = new FormAttachment(78, 0);
        this.wClusterName.setLayoutData(fdClusterName);
        this.wlBootstrapServers = new Label(wConnectionGroup, 16384);
        this.props.setLook(this.wlBootstrapServers);
        this.wlBootstrapServers.setText("Kafka地址：");
        FormData fdlBootstrapServers = new FormData();
        fdlBootstrapServers.left = new FormAttachment(environmentSeparator, 15);
        fdlBootstrapServers.top = new FormAttachment(0, 0);
        fdlBootstrapServers.right = new FormAttachment(78, 0);
        this.wlBootstrapServers.setLayoutData(fdlBootstrapServers);
        this.wBootstrapServers = new TextVar(this.transMeta, wConnectionGroup, 18436);
        this.props.setLook(this.wBootstrapServers);
        this.wBootstrapServers.addModifyListener(this.lsMod);
        FormData fdBootstrapServers = new FormData();
        fdBootstrapServers.left = new FormAttachment(this.wlBootstrapServers, 0, 16384);
        fdBootstrapServers.top = new FormAttachment(this.wlBootstrapServers, 5);
        fdBootstrapServers.right = new FormAttachment(78, 0);
        this.wBootstrapServers.setLayoutData(fdBootstrapServers);
        Label wlTopic = new Label(wSetupComp, 16384);
        this.props.setLook(wlTopic);
        wlTopic.setText("请选择主题（Topics）");
        FormData fdlTopic = new FormData();
        fdlTopic.left = new FormAttachment(0, 0);
        fdlTopic.top = new FormAttachment(wConnectionGroup, 10);
        fdlTopic.right = new FormAttachment(50, 0);
        wlTopic.setLayoutData(fdlTopic);
        this.wConsumerGroup = new TextVar(this.transMeta, wSetupComp, 18436);
        this.props.setLook(this.wConsumerGroup);
        this.wConsumerGroup.addModifyListener(this.lsMod);
        FormData fdConsumerGroup = new FormData();
        fdConsumerGroup.left = new FormAttachment(0, 0);
        fdConsumerGroup.bottom = new FormAttachment(100, 0);
        fdConsumerGroup.width = INPUT_WIDTH;
        this.wConsumerGroup.setLayoutData(fdConsumerGroup);
        Label wlConsumerGroup = new Label(wSetupComp, 16384);
        this.props.setLook(wlConsumerGroup);
        wlConsumerGroup.setText("消费组名称：");
        FormData fdlConsumerGroup = new FormData();
        fdlConsumerGroup.left = new FormAttachment(0, 0);
        fdlConsumerGroup.bottom = new FormAttachment(this.wConsumerGroup, -5, 128);
        fdlConsumerGroup.right = new FormAttachment(50, 0);
        wlConsumerGroup.setLayoutData(fdlConsumerGroup);
        buildTopicsTable(wSetupComp, wlTopic, wlConsumerGroup);
        FormData fdSetupComp = new FormData();
        fdSetupComp.left = new FormAttachment(0, 0);
        fdSetupComp.top = new FormAttachment(0, 0);
        fdSetupComp.right = new FormAttachment(100, 0);
        fdSetupComp.bottom = new FormAttachment(100, 0);
        wSetupComp.setLayoutData(fdSetupComp);
        wSetupComp.layout();
        this.wSetupTab.setControl(wSetupComp);
    }

    /* JADX INFO: Access modifiers changed from: private */
    public void toggleVisibility(boolean isDirect) {
        this.wlBootstrapServers.setVisible(isDirect);
        this.wBootstrapServers.setVisible(isDirect);
        this.wlClusterName.setVisible(!isDirect);
        this.wClusterName.setVisible(!isDirect);
    }

    private void buildFieldsTab() {
        CTabItem wFieldsTab = new CTabItem(this.wTabFolder, 0, 2);
        wFieldsTab.setText("字段配置");
        Composite wFieldsComp = new Composite(this.wTabFolder, 0);
        this.props.setLook(wFieldsComp);
        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = 15;
        fieldsLayout.marginWidth = 15;
        wFieldsComp.setLayout(fieldsLayout);
        FormData fieldsFormData = new FormData();
        fieldsFormData.left = new FormAttachment(0, 0);
        fieldsFormData.top = new FormAttachment(wFieldsComp, 0);
        fieldsFormData.right = new FormAttachment(100, 0);
        fieldsFormData.bottom = new FormAttachment(100, 0);
        wFieldsComp.setLayoutData(fieldsFormData);
        buildFieldTable(wFieldsComp, wFieldsComp);
        wFieldsComp.layout();
        wFieldsTab.setControl(wFieldsComp);
    }

    private void buildOptionsTab() {
        CTabItem wOptionsTab = new CTabItem(this.wTabFolder, 0);
        wOptionsTab.setText("选项配置");
        Composite wOptionsComp = new Composite(this.wTabFolder, 0);
        this.props.setLook(wOptionsComp);
        FormLayout fieldsLayout = new FormLayout();
        fieldsLayout.marginHeight = 15;
        fieldsLayout.marginWidth = 15;
        wOptionsComp.setLayout(fieldsLayout);
        FormData optionsFormData = new FormData();
        optionsFormData.left = new FormAttachment(0, 0);
        optionsFormData.top = new FormAttachment(wOptionsComp, 0);
        optionsFormData.right = new FormAttachment(100, 0);
        optionsFormData.bottom = new FormAttachment(100, 0);
        wOptionsComp.setLayoutData(optionsFormData);
        buildOptionsTable(wOptionsComp);
        wOptionsComp.layout();
        wOptionsTab.setControl(wOptionsComp);
    }

    private void buildFieldTable(Composite parentWidget, Control relativePosition) {
        ColumnInfo[] columns = getFieldColumns();
        int fieldCount = KafkaConsumerField.Name.values().length;
        this.fieldsTable = new TableView(this.transMeta, parentWidget, 67586, columns, fieldCount, true, this.lsMod, this.props, false);
        this.fieldsTable.setSortable(false);
        this.fieldsTable.getTable().addListener(11, event -> {
            Table table = (Table) event.widget;
            table.getColumn(1).setWidth(147);
            table.getColumn(2).setWidth(147);
            table.getColumn(3).setWidth(147);
        });
        populateFieldData();
        FormData fdData = new FormData();
        fdData.left = new FormAttachment(0, 0);
        fdData.top = new FormAttachment(relativePosition, 5);
        fdData.right = new FormAttachment(100, 0);
        Arrays.stream(this.fieldsTable.getTable().getColumns()).forEach(column -> {
            if (column.getWidth() > 0) {
                column.setWidth(120);
            }
        });
        this.fieldsTable.setReadonly(true);
        this.fieldsTable.setLayoutData(fdData);
    }

    private void buildOptionsTable(Composite parentWidget) {
        ColumnInfo[] columns = getOptionsColumns();
        if (this.consumerMeta.getConfig().size() == 0) {
            List<String> list = KafkaDialogHelper.getConsumerAdvancedConfigOptionNames();
            LinkedHashMap linkedHashMap = new LinkedHashMap();
            for (String item : list) {
                linkedHashMap.put(item, DEFAULT_OPTION_VALUES.getOrDefault(item, ""));
            }
            this.consumerMeta.setConfig(linkedHashMap);
        }
        int fieldCount = this.consumerMeta.getConfig().size();
        this.optionsTable = new TableView(this.transMeta, parentWidget, 67586, columns, fieldCount, false, this.lsMod, this.props, false);
        this.optionsTable.setSortable(false);
        this.optionsTable.getTable().addListener(11, event -> {
            Table table = (Table) event.widget;
            table.getColumn(1).setWidth(220);
            table.getColumn(2).setWidth(220);
        });
        populateOptionsData();
        FormData fdData = new FormData();
        fdData.left = new FormAttachment(0, 0);
        fdData.top = new FormAttachment(0, 0);
        fdData.right = new FormAttachment(100, 0);
        fdData.bottom = new FormAttachment(100, 0);
        Arrays.stream(this.optionsTable.getTable().getColumns()).forEach(column -> {
            if (column.getWidth() > 0) {
                column.setWidth(120);
            }
        });
        this.optionsTable.setLayoutData(fdData);
    }

    private ColumnInfo[] getFieldColumns() {
        KafkaConsumerField.Type[] values = KafkaConsumerField.Type.values();
        String[] supportedTypes = (String[]) Arrays.stream(values).map((v0) -> {
            return v0.toString();
        }).toArray(x$0 -> {
            return new String[x$0];
        });
        ColumnInfo referenceName = new ColumnInfo("输入字段名称", 1, false, true);
        ColumnInfo name = new ColumnInfo("输出字段名称", 1, false, false);
        ColumnInfo type = new ColumnInfo("字段类型", 2, supportedTypes, false);
        type.setDisabledListener(rowNumber -> {
            String ref = this.fieldsTable.getTable().getItem(rowNumber).getText(1);
            KafkaConsumerField.Name refName = KafkaConsumerField.Name.valueOf(ref.toUpperCase());
            return (refName == KafkaConsumerField.Name.KEY || refName == KafkaConsumerField.Name.MESSAGE) ? false : true;
        });
        return new ColumnInfo[]{referenceName, name, type};
    }

    private ColumnInfo[] getOptionsColumns() {
        ColumnInfo optionName = new ColumnInfo("名称", 1, false, false);
        ColumnInfo value = new ColumnInfo("值", 1, false, false);
        value.setUsingVariables(true);
        return new ColumnInfo[]{optionName, value};
    }

    private void populateFieldData() {
        List<KafkaConsumerField> fieldDefinitions = this.consumerMeta.getFieldDefinitions();
        int rowIndex = 0;
        for (KafkaConsumerField field : fieldDefinitions) {
            int i = rowIndex;
            rowIndex++;
            TableItem key = this.fieldsTable.getTable().getItem(i);
            if (field.getKafkaName() != null) {
                key.setText(1, field.getKafkaName().toString());
            }
            if (field.getOutputName() != null) {
                key.setText(2, field.getOutputName());
            }
            if (field.getOutputType() != null) {
                key.setText(3, field.getOutputType().toString());
            }
        }
    }

    private void populateOptionsData() {
        int rowIndex = 0;
        for (Map.Entry<String, String> entry : this.consumerMeta.getConfig().entrySet()) {
            int i = rowIndex;
            rowIndex++;
            TableItem key = this.optionsTable.getTable().getItem(i);
            key.setText(1, entry.getKey());
            key.setText(2, entry.getValue() == null ? "" : entry.getValue());
        }
    }

    private void populateTopicsData() {
        List<String> topics = this.consumerMeta.getTopics();
        int rowIndex = 0;
        for (String topic : topics) {
            int i = rowIndex;
            rowIndex++;
            TableItem key = this.topicsTable.getTable().getItem(i);
            if (topic != null) {
                key.setText(1, topic);
            }
        }
    }

    private void buildTopicsTable(Composite parentWidget, Control controlAbove, Control controlBelow) {
        ColumnInfo[] columns = {new ColumnInfo("主题名称", 2, new String[1], false)};
        columns[0].setUsingVariables(true);
        int topicsCount = this.consumerMeta.getTopics().size();
        Listener lsFocusInTopic = e -> {
            if (!(e.widget instanceof CCombo)) {
                return;
            }
            CCombo ccom = (CCombo) e.widget;
            if (!(ccom.getParent() instanceof ComboVar)) {
                return;
            }
            ComboVar cvar = (ComboVar) ccom.getParent();
            KafkaDialogHelper kdh = new KafkaDialogHelper(this.wClusterName, cvar, this.wbCluster, this.wBootstrapServers, this.kafkaFactory, this.optionsTable, this.meta.getParentStepMeta());
            kdh.clusterNameChanged(e);
        };
        this.topicsTable = new TableView(this.transMeta, parentWidget, 67586, columns, topicsCount, false, this.lsMod, this.props, false, lsFocusInTopic);
        this.topicsTable.setSortable(false);
        this.topicsTable.getTable().addListener(11, event -> {
            Table table = (Table) event.widget;
            table.getColumn(1).setWidth(316);
        });
        populateTopicsData();
        FormData fdData = new FormData();
        fdData.left = new FormAttachment(0, 0);
        fdData.top = new FormAttachment(controlAbove, 5);
        fdData.right = new FormAttachment(0, 337);
        fdData.bottom = new FormAttachment(controlBelow, -10, 128);
        Arrays.stream(this.topicsTable.getTable().getColumns()).forEach(column -> {
            if (column.getWidth() > 0) {
                column.setWidth(120);
            }
        });
        this.topicsTable.setLayoutData(fdData);
    }

    protected void getData() {
        if (this.meta.getTransformationPath() != null) {
            this.wFileSection.wFileName.setText(this.meta.getTransformationPath());
        }
        // 命名集群依赖 Pentaho Big Data；本插件仅保留 UI 占位，下拉为空时请使用「直连」模式。
        this.wClusterName.setItems(new String[0]);
        if (this.consumerMeta.getClusterName() != null) {
            this.wClusterName.setText(this.consumerMeta.getClusterName());
        }
        if (this.consumerMeta.getDirectBootstrapServers() != null) {
            this.wBootstrapServers.setText(this.consumerMeta.getDirectBootstrapServers());
        }
        populateTopicsData();
        if (this.consumerMeta.getSubStep() != null) {
            this.wSubStep.setText(this.consumerMeta.getSubStep());
        }
        if (this.consumerMeta.getConsumerGroup() != null) {
            this.wConsumerGroup.setText(this.consumerMeta.getConsumerGroup());
        }
        if (this.meta.getBatchSize() != null) {
            this.wBatchSize.setText(this.meta.getBatchSize());
        }
        if (this.meta.getBatchDuration() != null) {
            this.wBatchDuration.setText(this.meta.getBatchDuration());
        }
        if (this.meta.getParallelism() != null) {
            this.wParallelism.setText(this.meta.getParallelism());
        }
        if (this.meta.getPrefetchCount() != null) {
            this.wPrefetchCount.setText(this.meta.getPrefetchCount());
        }
        this.wbCluster.setSelection(!isDirect());
        this.wbDirect.setSelection(isDirect());
        toggleVisibility(isDirect());
        this.wbAutoCommit.setSelection(this.consumerMeta.isAutoCommit());
        this.wbManualCommit.setSelection(!this.consumerMeta.isAutoCommit());
        this.specificationMethod = this.meta.getSpecificationMethod();
        switch (AnonymousClass3.$SwitchMap$org$pentaho$di$core$ObjectLocationSpecificationMethod[this.specificationMethod.ordinal()]) {
            case 1:
                this.wFileSection.wFileName.setText(Const.NVL(this.meta.getFileName(), ""));
                break;
            case 2:
                String fullPath = Const.NVL(this.meta.getDirectoryPath(), "") + REPOS_DELIM + Const.NVL(this.meta.getTransName(), "");
                this.wFileSection.wFileName.setText(fullPath);
                break;
        }
        populateFieldData();
    }

    /* JADX INFO: renamed from: com.lsd.dtdesigner.plugin.bigdata.kafka.KafkaConsumerInputDialog$3, reason: invalid class name */
    /* JADX INFO: loaded from: lsd-bigdata-plugin-2.3.2.jar:com/lsd/dtdesigner/plugin/bigdata/kafka/KafkaConsumerInputDialog$3.class */
    static /* synthetic */ class AnonymousClass3 {
        static final /* synthetic */ int[] $SwitchMap$org$pentaho$di$core$ObjectLocationSpecificationMethod = new int[ObjectLocationSpecificationMethod.values().length];

        static {
            try {
                $SwitchMap$org$pentaho$di$core$ObjectLocationSpecificationMethod[ObjectLocationSpecificationMethod.FILENAME.ordinal()] = 1;
            } catch (NoSuchFieldError e) {
            }
            try {
                $SwitchMap$org$pentaho$di$core$ObjectLocationSpecificationMethod[ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME.ordinal()] = 2;
            } catch (NoSuchFieldError e2) {
            }
        }
    }

    private boolean isDirect() {
        return KafkaConsumerInputMeta.ConnectionType.DIRECT.equals(this.consumerMeta.getConnectionType());
    }

    protected void additionalOks(BaseStreamStepMeta meta) {
        setTopicsFromTable();
        this.consumerMeta.setClusterName(this.wClusterName.getText());
        this.consumerMeta.setConsumerGroup(this.wConsumerGroup.getText());
        this.consumerMeta.setConnectionType(this.wbDirect.getSelection() ? KafkaConsumerInputMeta.ConnectionType.DIRECT : KafkaConsumerInputMeta.ConnectionType.CLUSTER);
        this.consumerMeta.setDirectBootstrapServers(this.wBootstrapServers.getText());
        this.consumerMeta.setAutoCommit(this.wbAutoCommit.getSelection());
        setFieldsFromTable();
        setOptionsFromTable();
    }

    private void setFieldsFromTable() {
        int itemCount = this.fieldsTable.getItemCount();
        for (int rowIndex = 0; rowIndex < itemCount; rowIndex++) {
            TableItem row = this.fieldsTable.getTable().getItem(rowIndex);
            String kafkaName = row.getText(1);
            String outputName = row.getText(2);
            String outputType = row.getText(3);
            try {
                KafkaConsumerField.Name ref = KafkaConsumerField.Name.valueOf(kafkaName.toUpperCase());
                KafkaConsumerField field = new KafkaConsumerField(ref, outputName, KafkaConsumerField.Type.valueOf(outputType));
                this.consumerMeta.setField(field);
            } catch (IllegalArgumentException e) {
                if (isDebug()) {
                    logDebug(e.getMessage(), new Object[]{e});
                }
            }
        }
    }

    private void setTopicsFromTable() {
        int itemCount = this.topicsTable.getItemCount();
        ArrayList<String> tableTopics = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < itemCount; rowIndex++) {
            TableItem row = this.topicsTable.getTable().getItem(rowIndex);
            String topic = row.getText(1);
            if (!"".equals(topic) && tableTopics.indexOf(topic) == -1) {
                tableTopics.add(topic);
            }
        }
        this.consumerMeta.setTopics(tableTopics);
    }

    private void setOptionsFromTable() {
        this.consumerMeta.setConfig(KafkaDialogHelper.getConfig(this.optionsTable));
    }
}