package com.pufferfishscheduler.plugin;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamSource extends BlockingQueueStreamSource<List<Object>> {
    private final Logger logger;
    private final VariableSpace variables;
    private KafkaConsumerInputMeta kafkaConsumerInputMeta;
    private KafkaConsumerInputData kafkaConsumerInputData;
    private EnumMap<KafkaConsumerField.Name, Integer> positions;
    private Consumer consumer;
    private final ExecutorService executorService;
    private KafkaConsumerCallable callable;
    private Future<Void> future;

    public KafkaStreamSource(Consumer consumer, KafkaConsumerInputMeta inputMeta, KafkaConsumerInputData kafkaConsumerInputData, VariableSpace variables, KafkaConsumerInput kafkaStep) {
        super(kafkaStep);
        this.logger = LoggerFactory.getLogger(getClass());
        this.executorService = Executors.newCachedThreadPool();
        this.positions = new EnumMap<>(KafkaConsumerField.Name.class);
        this.consumer = consumer;
        this.variables = variables;
        this.kafkaConsumerInputData = kafkaConsumerInputData;
        this.kafkaConsumerInputMeta = inputMeta;
    }

    public void close() {
        this.callable.shutdown();
    }

    public void open() {
        if (this.future != null) {
            this.logger.warn("open() called more than once");
            return;
        }
        List<ValueMetaInterface> valueMetas = this.kafkaConsumerInputData.outputRowMeta.getValueMetaList();
        this.positions = new EnumMap<>(KafkaConsumerField.Name.class);
        IntStream.range(0, valueMetas.size()).forEach(idx -> {
            Optional<KafkaConsumerField.Name> match = Arrays.stream(KafkaConsumerField.Name.values()).filter(name -> {
                KafkaConsumerField f = name.getFieldFromMeta(this.kafkaConsumerInputMeta);
                String fieldName = this.variables.environmentSubstitute(f.getOutputName());
                return fieldName != null && fieldName.equals(((ValueMetaInterface) valueMetas.get(idx)).getName());
            }).findFirst();
            match.ifPresent(name2 -> {
                this.positions.put(name2, Integer.valueOf(idx));
            });
        });
        this.callable = new KafkaConsumerCallable(this.consumer, () -> {
            super.close();
        });
        this.future = this.executorService.submit(this.callable);
    }

    /* JADX INFO: loaded from: lsd-bigdata-plugin-2.3.2.jar:com/lsd/dtdesigner/plugin/bigdata/kafka/KafkaStreamSource$KafkaConsumerCallable.class */
    class KafkaConsumerCallable implements Callable<Void> {
        private final Consumer consumer;
        private Runnable onClose;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommit = new ConcurrentLinkedQueue<>();

        public KafkaConsumerCallable(Consumer consumer, Runnable onClose) {
            this.consumer = consumer;
            this.onClose = onClose;
        }

        public void queueCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.toCommit.add(offsets);
        }

        /* JADX WARN: Can't rename method to resolve collision */
        @Override // java.util.concurrent.Callable
        public Void call() {
            while (!this.closed.get()) {
                try {
                    try {
                        try {
                            commitOffsets();
                            ConsumerRecords<String, String> records = this.consumer.poll(1000L);
                            List<List<Object>> rows = new ArrayList<>();
                            Iterator it = records.iterator();
                            while (it.hasNext()) {
                                ConsumerRecord<String, String> record = (ConsumerRecord) it.next();
                                rows.add(KafkaStreamSource.this.processMessageAsRow(record));
                            }
                            KafkaStreamSource.this.acceptRows(rows);
                        } catch (WakeupException e) {
                            if (!this.closed.get()) {
                                throw e;
                            }
                            commitOffsets();
                            this.consumer.close();
                            this.onClose.run();
                            return null;
                        }
                    } catch (Exception ef) {
                        KafkaStreamSource.this.streamStep.logError("Exception consuming messages.", ef);
                        commitOffsets();
                        this.consumer.close();
                        this.onClose.run();
                        return null;
                    }
                } catch (Throwable th) {
                    commitOffsets();
                    this.consumer.close();
                    this.onClose.run();
                    throw th;
                }
            }
            commitOffsets();
            this.consumer.close();
            this.onClose.run();
            return null;
        }

        private void commitOffsets() {
            while (!this.toCommit.isEmpty()) {
                this.consumer.commitSync(this.toCommit.poll());
            }
        }

        public void shutdown() {
            this.closed.set(true);
            this.consumer.wakeup();
        }
    }

    public void commitOffsets(List<List<Object>> rows) {
        Map<Object, Map<Object, Optional<List<Object>>>> maxRows = (Map) rows.stream().collect(Collectors.groupingBy(row -> {
            return row.get(this.positions.get(KafkaConsumerField.Name.TOPIC).intValue());
        }, Collectors.groupingBy(row2 -> {
            return row2.get(this.positions.get(KafkaConsumerField.Name.PARTITION).intValue());
        }, Collectors.maxBy(Comparator.comparingLong(row3 -> {
            return ((Long) row3.get(this.positions.get(KafkaConsumerField.Name.OFFSET).intValue())).longValue();
        })))));
        Map<TopicPartition, OffsetAndMetadata> offsets = (Map) maxRows.values().stream().flatMap(m -> {
            return m.values().stream();
        }).map((v0) -> {
            return v0.get();
        }).collect(Collectors.toMap(row4 -> {
            return new TopicPartition((String) row4.get(this.positions.get(KafkaConsumerField.Name.TOPIC).intValue()), ((Long) row4.get(this.positions.get(KafkaConsumerField.Name.PARTITION).intValue())).intValue());
        }, row5 -> {
            return new OffsetAndMetadata(((Long) row5.get(this.positions.get(KafkaConsumerField.Name.OFFSET).intValue())).longValue() + 1);
        }));
        this.callable.queueCommit(offsets);
    }

    List<Object> processMessageAsRow(ConsumerRecord<String, String> record) {
        Object[] rowData = RowDataUtil.allocateRowData(this.kafkaConsumerInputData.outputRowMeta.size());
        if (this.positions.get(KafkaConsumerField.Name.KEY) != null) {
            rowData[this.positions.get(KafkaConsumerField.Name.KEY).intValue()] = record.key();
        }
        if (this.positions.get(KafkaConsumerField.Name.MESSAGE) != null) {
            rowData[this.positions.get(KafkaConsumerField.Name.MESSAGE).intValue()] = record.value();
        }
        if (this.positions.get(KafkaConsumerField.Name.TOPIC) != null) {
            rowData[this.positions.get(KafkaConsumerField.Name.TOPIC).intValue()] = record.topic();
        }
        if (this.positions.get(KafkaConsumerField.Name.PARTITION) != null) {
            rowData[this.positions.get(KafkaConsumerField.Name.PARTITION).intValue()] = Long.valueOf(record.partition());
        }
        if (this.positions.get(KafkaConsumerField.Name.OFFSET) != null) {
            rowData[this.positions.get(KafkaConsumerField.Name.OFFSET).intValue()] = Long.valueOf(record.offset());
        }
        if (this.positions.get(KafkaConsumerField.Name.TIMESTAMP) != null) {
            rowData[this.positions.get(KafkaConsumerField.Name.TIMESTAMP).intValue()] = Long.valueOf(record.timestamp());
        }
        return Arrays.asList(rowData);
    }
}