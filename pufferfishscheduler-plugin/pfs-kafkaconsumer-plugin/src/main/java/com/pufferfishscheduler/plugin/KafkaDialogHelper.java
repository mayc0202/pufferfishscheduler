package com.pufferfishscheduler.plugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;

/**
 * 主题下拉、字段下拉等 UI 辅助（仅直连 bootstrap；不依赖 Named Cluster / Metastore）。
 */
public class KafkaDialogHelper {
    private ComboVar wTopic;
    private ComboVar wClusterName;
    private Button wbCluster;
    private TextVar wBootstrapServers;
    private KafkaFactory kafkaFactory;
    private TableView optionsTable;
    private StepMeta parentMeta;

    KafkaDialogHelper(ComboVar wClusterName, ComboVar wTopic, Button wbCluster, TextVar wBootstrapServers,
            KafkaFactory kafkaFactory, TableView optionsTable, StepMeta parentMeta) {
        this.wClusterName = wClusterName;
        this.wTopic = wTopic;
        this.wbCluster = wbCluster;
        this.wBootstrapServers = wBootstrapServers;
        this.kafkaFactory = kafkaFactory;
        this.optionsTable = optionsTable;
        this.parentMeta = parentMeta;
    }

    public void clusterNameChanged(Event event) {
        if (!this.wbCluster.getSelection() || !StringUtil.isEmpty(this.wClusterName.getText())) {
            if (!this.wbCluster.getSelection() && StringUtil.isEmpty(this.wBootstrapServers.getText())) {
                return;
            }
            String current = this.wTopic.getText();
            String clusterName = this.wClusterName.getText();
            boolean isCluster = this.wbCluster.getSelection();
            String directBootstrapServers = this.wBootstrapServers == null ? "" : this.wBootstrapServers.getText();
            Map<String, String> config = getConfig(this.optionsTable);
            if (!this.wTopic.getCComboWidget().isDisposed()) {
                this.wTopic.getCComboWidget().removeAll();
            }
            CompletableFuture.supplyAsync(() -> listTopics(clusterName, isCluster, directBootstrapServers, config))
                    .thenAccept(topicMap -> Display.getDefault().syncExec(() -> populateTopics(topicMap, current)));
        }
    }

    private void populateTopics(Map<String, List<PartitionInfo>> topicMap, String current) {
        topicMap.keySet().stream()
                .filter(key -> !"__consumer_offsets".equals(key))
                .sorted()
                .forEach(key2 -> {
                    if (!this.wTopic.isDisposed()) {
                        this.wTopic.add(key2);
                    }
                });
        if (!this.wTopic.getCComboWidget().isDisposed()) {
            this.wTopic.getCComboWidget().setText(current);
        }
    }

    private Map<String, List<PartitionInfo>> listTopics(String clusterName, boolean isCluster,
            String directBootstrapServers, Map<String, String> config) {
        Consumer kafkaConsumer = null;
        try {
            KafkaConsumerInputMeta localMeta = new KafkaConsumerInputMeta();
            localMeta.setConnectionType(isCluster ? KafkaConsumerInputMeta.ConnectionType.CLUSTER
                    : KafkaConsumerInputMeta.ConnectionType.DIRECT);
            localMeta.setClusterName(clusterName);
            localMeta.setDirectBootstrapServers(directBootstrapServers);
            localMeta.setConfig(config);
            localMeta.setParentStepMeta(this.parentMeta);
            kafkaConsumer = this.kafkaFactory.consumer(localMeta, Function.identity());
            return kafkaConsumer.listTopics();
        } catch (Exception e) {
            return Collections.emptyMap();
        } finally {
            if (kafkaConsumer != null) {
                try {
                    kafkaConsumer.close();
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }
    }

    public static void populateFieldsList(TransMeta transMeta, ComboVar comboVar, String stepName) {
        String current = comboVar.getText();
        comboVar.getCComboWidget().removeAll();
        comboVar.setText(current);
        try {
            RowMetaInterface rmi = transMeta.getPrevStepFields(stepName);
            List<?> ls = rmi.getValueMetaList();
            for (Object l : ls) {
                ValueMetaBase vmb = (ValueMetaBase) l;
                comboVar.add(vmb.getName());
            }
        } catch (KettleStepException e) {
            // ignore
        }
    }

    public static List<String> getConsumerAdvancedConfigOptionNames() {
        return Arrays.asList("auto.offset.reset", "ssl.key.password", "ssl.keystore.location", "ssl.keystore.password",
                "ssl.truststore.location", "ssl.truststore.password");
    }

    public static List<String> getProducerAdvancedConfigOptionNames() {
        return Arrays.asList("compression.type", "ssl.key.password", "ssl.keystore.location", "ssl.keystore.password",
                "ssl.truststore.location", "ssl.truststore.password");
    }

    public static Map<String, String> getConfig(TableView optionsTable) {
        int itemCount = optionsTable.getItemCount();
        Map<String, String> advancedConfig = new LinkedHashMap<>();
        for (int rowIndex = 0; rowIndex < itemCount; rowIndex++) {
            TableItem row = optionsTable.getTable().getItem(rowIndex);
            String config = row.getText(1);
            String value = row.getText(2);
            if (!StringUtils.isBlank(config) && !advancedConfig.containsKey(config)) {
                advancedConfig.put(config, value);
            }
        }
        return advancedConfig;
    }
}
