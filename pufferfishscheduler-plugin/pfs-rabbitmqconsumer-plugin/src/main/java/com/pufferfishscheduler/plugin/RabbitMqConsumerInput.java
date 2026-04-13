package com.pufferfishscheduler.plugin;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * RabbitMQ 队列订阅：在独立线程上建立 {@link Channel} 消费，主线程 {@link #processRow} 取投递并输出；
 * 手动确认时在同一串行执行器上 ack/nack，避免 Channel 跨线程使用。
 */
public class RabbitMqConsumerInput extends BaseStep implements StepInterface {

    public static final class QueuedDelivery {
        public final long deliveryTag;
        public final byte[] body;
        public final AMQP.BasicProperties properties;
        public final String exchange;
        public final String routingKey;
        public final boolean redelivered;

        public QueuedDelivery(
                long deliveryTag,
                byte[] body,
                AMQP.BasicProperties properties,
                String exchange,
                String routingKey,
                boolean redelivered) {
            this.deliveryTag = deliveryTag;
            this.body = body;
            this.properties = properties;
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.redelivered = redelivered;
        }
    }

    private RabbitMqConsumerInputMeta meta;
    private RabbitMqConsumerInputData data;

    private Connection connection;
    private Channel channel;
    private ExecutorService channelExecutor;
    private final List<String> consumerTags = new ArrayList<>();

    /** 当前行对应的上游数据（非独立输入时由 getRow() 提供），用于重试时拼装输出行 */
    private Object[] currentUpstreamRow;

    public RabbitMqConsumerInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
            Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }
        this.meta = (RabbitMqConsumerInputMeta) smi;
        this.data = (RabbitMqConsumerInputData) sdi;
        this.data.standaloneRunCompleted = false;
        this.data.messagesEmitted = 0L;

        String q = environmentSubstitute(meta.getQueue());
        if (StringUtils.isBlank(q)) {
            logError("RabbitMQ 队列名不能为空");
            return false;
        }
        String host = environmentSubstitute(meta.getHost());
        if (StringUtils.isBlank(host)) {
            logError("RabbitMQ 主机不能为空");
            return false;
        }

        int capacity = Math.max(64, meta.getPrefetch() * meta.effectiveConsumerCount() * 8);
        this.data.deliveryQueue = new java.util.concurrent.LinkedBlockingQueue<>(capacity);

        this.channelExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rabbitmq-consumer-" + getStepname());
            t.setDaemon(true);
            return t;
        });

        try {
            ConnectionFactory factory = RabbitMqConnectionSupport.buildFactory(
                    meta.getHost(),
                    meta.getPort(),
                    meta.getUsername(),
                    meta.getPassword(),
                    meta.getVirtualHost(),
                    meta.getConfig(),
                    this::environmentSubstitute);
            Future<?> fut = channelExecutor.submit((Callable<Void>) () -> {
                connection = factory.newConnection();
                channel = connection.createChannel();
                int qos = Math.max(1, meta.getPrefetch());
                channel.basicQos(qos);
                String queueName = environmentSubstitute(meta.getQueue());
                boolean autoAck = meta.isAutoAck();
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    QueuedDelivery qd = new QueuedDelivery(
                            delivery.getEnvelope().getDeliveryTag(),
                            delivery.getBody(),
                            delivery.getProperties(),
                            delivery.getEnvelope().getExchange(),
                            delivery.getEnvelope().getRoutingKey(),
                            delivery.getEnvelope().isRedeliver());
                    try {
                        if (!data.deliveryQueue.offer(qd, 30, TimeUnit.SECONDS)) {
                            if (!autoAck) {
                                channel.basicNack(
                                        delivery.getEnvelope().getDeliveryTag(), false, meta.isDefaultRequeueRejected());
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        try {
                            if (!autoAck) {
                                channel.basicNack(
                                        delivery.getEnvelope().getDeliveryTag(), false, meta.isDefaultRequeueRejected());
                            }
                        } catch (Exception ignored) {
                            // ignore
                        }
                    }
                };
                CancelCallback cancelCallback = consumerTag -> { };
                int n = meta.effectiveConsumerCount();
                for (int i = 0; i < n; i++) {
                    String tag = channel.basicConsume(queueName, autoAck, deliverCallback, cancelCallback);
                    consumerTags.add(tag);
                }
                return null;
            });
            fut.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logError("RabbitMQ 消费初始化超时", e);
            return false;
        } catch (Exception e) {
            logError("RabbitMQ 消费初始化失败", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (RabbitMqConsumerInputMeta) smi;
        data = (RabbitMqConsumerInputData) sdi;

        Object[] upstream = getRow();
        this.currentUpstreamRow = upstream;

        if (first) {
            RowMetaInterface inputMeta = getInputRowMeta();
            if (inputMeta == null) {
                data.inputRowMeta = new RowMeta();
            } else {
                data.inputRowMeta = inputMeta;
            }
            data.outputRowMeta = data.inputRowMeta.clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
            first = false;
        }

        if (shouldStopProcessing(upstream)) {
            shutdownConsumersQuietly();
            setOutputDone();
            return false;
        }

        if (meta.getMaxMessages() > 0 && data.messagesEmitted >= meta.getMaxMessages()) {
            data.standaloneRunCompleted = true;
            shutdownConsumersQuietly();
            setOutputDone();
            return false;
        }

        try {
            long waitSeconds = isStandaloneInput() ? 1L : 60L;
            QueuedDelivery d = data.deliveryQueue.poll(waitSeconds, TimeUnit.SECONDS);
            if (d == null) {
                if (isStopped()) {
                    shutdownConsumersQuietly();
                    setOutputDone();
                    return false;
                }
                if (!isStandaloneInput()) {
                    logDebug("等待队列消息超时: " + environmentSubstitute(meta.getQueue()));
                }
                return true;
            }

            Object[] out = buildOutputRow(d, isStandaloneInput() ? null : upstream);
            boolean ok = false;
            try {
                putRow(data.outputRowMeta, out);
                ok = true;
            } catch (KettleStepException e) {
                logError("下游处理失败", e);
            }

            if (meta.isAutoAck()) {
                if (ok) {
                    data.messagesEmitted++;
                }
                return !isStopped();
            }

            if (ok) {
                ack(d.deliveryTag);
                data.messagesEmitted++;
            } else {
                handleFailureWithRetry(d);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shutdownConsumersQuietly();
            setOutputDone();
            return false;
        }

        return !isStopped();
    }

    private Object[] buildOutputRow(QueuedDelivery d, Object[] upstream) throws KettleException {
        int base = data.inputRowMeta.size();
        if (isStandaloneInput()) {
            base = 0;
        } else if (upstream != null && upstream.length < base) {
            base = upstream.length;
        }
        List<RabbitMqConsumerField> defs = meta.getFieldDefinitions();
        int extra = 0;
        for (RabbitMqConsumerField f : defs) {
            if (!Utils.isEmpty(f.getOutputName())) {
                extra++;
            }
        }
        Object[] row = new Object[base + extra];
        if (base > 0 && upstream != null) {
            System.arraycopy(upstream, 0, row, 0, base);
        }
        int idx = base;
        for (RabbitMqConsumerField f : defs) {
            if (Utils.isEmpty(f.getOutputName())) {
                continue;
            }
            row[idx++] = extractValue(d, f);
        }
        return row;
    }

    private static Object extractValue(QueuedDelivery d, RabbitMqConsumerField f) {
        return switch (f.getRabbitName()) {
            case MESSAGE -> {
                if (f.getOutputType() == RabbitMqConsumerField.Type.Binary) {
                    yield d.body;
                }
                yield d.body == null ? null : new String(d.body, StandardCharsets.UTF_8);
            }
            case ROUTING_KEY -> d.routingKey;
            case MESSAGE_ID -> d.properties != null ? d.properties.getMessageId() : null;
            case DELIVERY_TAG -> String.valueOf(d.deliveryTag);
            case EXCHANGE -> d.exchange;
            case TIMESTAMP -> {
                if (d.properties != null && d.properties.getTimestamp() != null) {
                    yield d.properties.getTimestamp().getTime();
                }
                yield null;
            }
        };
    }

    private void handleFailureWithRetry(QueuedDelivery d) throws KettleException {
        if (!meta.isRetryEnabled()) {
            nack(d.deliveryTag, meta.isDefaultRequeueRejected());
            return;
        }
        long delay = meta.getRetryInitialIntervalMs();
        Object[] upstreamForRetry = isStandaloneInput() ? null : currentUpstreamRow;
        for (int attempt = 1; attempt <= meta.getRetryMaxAttempts(); attempt++) {
            try {
                Thread.sleep(Math.min(delay, meta.getRetryMaxIntervalMs()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                nack(d.deliveryTag, meta.isDefaultRequeueRejected());
                return;
            }
            Object[] out;
            try {
                out = buildOutputRow(d, upstreamForRetry);
            } catch (KettleException e) {
                nack(d.deliveryTag, meta.isDefaultRequeueRejected());
                return;
            }
            try {
                putRow(data.outputRowMeta, out);
                ack(d.deliveryTag);
                data.messagesEmitted++;
                return;
            } catch (KettleStepException e) {
                logError("重试仍失败 (" + attempt + "/" + meta.getRetryMaxAttempts() + ")", e);
            }
            delay = Math.min(
                    (long) (delay * (meta.getRetryMultiplier() <= 0 ? 1.0 : meta.getRetryMultiplier())),
                    meta.getRetryMaxIntervalMs());
        }
        nack(d.deliveryTag, meta.isDefaultRequeueRejected());
    }

    private void ack(long deliveryTag) throws KettleException {
        if (meta.isAutoAck() || channelExecutor == null) {
            return;
        }
        try {
            channelExecutor.submit((Callable<Void>) () -> {
                        if (channel != null && channel.isOpen()) {
                            channel.basicAck(deliveryTag, false);
                        }
                        return null;
                    })
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new KettleException("RabbitMQ basicAck 失败", e);
        }
    }

    private void nack(long deliveryTag, boolean requeue) throws KettleException {
        if (meta.isAutoAck() || channelExecutor == null) {
            return;
        }
        try {
            channelExecutor.submit((Callable<Void>) () -> {
                        if (channel != null && channel.isOpen()) {
                            channel.basicNack(deliveryTag, false, requeue);
                        }
                        return null;
                    })
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new KettleException("RabbitMQ basicNack 失败", e);
        }
    }

    private boolean isStandaloneInput() {
        StepMeta[] prev = getTransMeta().getPrevSteps(getStepMeta());
        return prev == null || prev.length == 0;
    }

    private boolean shouldStopProcessing(Object[] upstream) {
        if (data.standaloneRunCompleted) {
            return true;
        }
        if (isStandaloneInput()) {
            return false;
        }
        return upstream == null;
    }

    private void shutdownConsumersQuietly() {
        if (channelExecutor == null) {
            return;
        }
        try {
            channelExecutor.submit((Callable<Void>) () -> {
                        if (channel != null && channel.isOpen()) {
                            for (String t : consumerTags) {
                                try {
                                    channel.basicCancel(t);
                                } catch (Exception ignored) {
                                    // ignore
                                }
                            }
                            channel.close();
                            channel = null;
                        }
                        if (connection != null && connection.isOpen()) {
                            connection.close();
                            connection = null;
                        }
                        return null;
                    })
                    .get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logDebug("关闭 RabbitMQ 消费连接时异常: " + e.getMessage());
        } finally {
            consumerTags.clear();
            channelExecutor.shutdown();
            try {
                channelExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            channelExecutor = null;
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        shutdownConsumersQuietly();
        super.dispose(smi, sdi);
    }
}
