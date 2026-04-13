package com.pufferfishscheduler.plugin;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * 按行发布至 RabbitMQ；支持 publisher confirms 与 mandatory+returns。
 */
public class RabbitMqProducerOutput extends BaseStep implements StepInterface {

    private RabbitMqProducerOutputMeta meta;
    private RabbitMqProducerOutputData data;

    public RabbitMqProducerOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
            Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        super.init(smi, sdi);
        this.meta = (RabbitMqProducerOutputMeta) smi;
        this.data = (RabbitMqProducerOutputData) sdi;
        return true;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        Object[] r = getRow();
        if (r == null) {
            setOutputDone();
            closeQuietly();
            return false;
        }
        if (first) {
            this.meta = (RabbitMqProducerOutputMeta) smi;
            this.data = (RabbitMqProducerOutputData) sdi;
            this.data.messageFieldIndex = getInputRowMeta().indexOfValue(environmentSubstitute(meta.getMessageField()));
            if (StringUtils.isNotBlank(meta.getRoutingKeyField())) {
                this.data.routingKeyFieldIndex =
                        getInputRowMeta().indexOfValue(environmentSubstitute(meta.getRoutingKeyField()));
            } else {
                this.data.routingKeyFieldIndex = -1;
            }
            openChannelIfNeeded();
            first = false;
        }
        if (!data.open) {
            return false;
        }

        ValueMetaInterface msgMeta = getInputRowMeta().getValueMeta(data.messageFieldIndex);
        Object mv = r[data.messageFieldIndex];
        byte[] body;
        if (msgMeta.getType() == ValueMetaInterface.TYPE_BINARY && mv instanceof byte[]) {
            body = (byte[]) mv;
        } else {
            String s = msgMeta.getString(mv);
            body = s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
        }

        String rk = environmentSubstitute(meta.getRoutingKey());
        if (data.routingKeyFieldIndex >= 0) {
            String fromRow = getInputRowMeta().getString(r, data.routingKeyFieldIndex);
            if (StringUtils.isNotBlank(fromRow)) {
                rk = fromRow;
            }
        }

        String exchange = environmentSubstitute(meta.getExchange());
        if (exchange == null) {
            exchange = "";
        }

        AMQP.BasicProperties props =
                new AMQP.BasicProperties.Builder().contentType("text/plain").deliveryMode(2).build();

        try {
            publishWithOptions(exchange, rk, props, body);
        } catch (Exception e) {
            logError("RabbitMQ 发布失败: " + e.getMessage(), e);
            setErrors(getErrors() + 1);
            closeQuietly();
            stopAll();
            return false;
        }

        putRow(getInputRowMeta(), r);
        return true;
    }

    private void openChannelIfNeeded() throws KettleException {
        if (data.open) {
            return;
        }
        try {
            ConnectionFactory factory = RabbitMqConnectionSupport.buildFactory(
                    meta.getHost(),
                    meta.getPort(),
                    meta.getUsername(),
                    meta.getPassword(),
                    meta.getVirtualHost(),
                    meta.getConfig(),
                    this::environmentSubstitute);
            Connection conn = factory.newConnection();
            Channel ch = conn.createChannel();
            if (meta.getPublisherConfirmType() != RabbitMqProducerOutputMeta.PublisherConfirmType.NONE) {
                ch.confirmSelect();
            }
            if (meta.isPublisherReturns() && meta.isMandatory()) {
                ch.addReturnListener(
                        (replyCode, replyText, exchange, routingKey, properties, body) ->
                                data.returnSignals.offer(replyText != null ? replyText : "returned"));
            }
            data.connection = conn;
            data.channel = ch;
            data.open = true;
        } catch (Exception e) {
            throw new KettleException("无法连接 RabbitMQ", e);
        }
    }

    private void publishWithOptions(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body)
            throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
        Channel ch = data.channel;
        data.returnSignals.clear();
        ch.basicPublish(exchange, routingKey == null ? "" : routingKey, meta.isMandatory(), props, body);

        if (meta.isPublisherReturns() && meta.isMandatory()) {
            String ret = data.returnSignals.poll(800, TimeUnit.MILLISECONDS);
            if (ret != null) {
                throw new IOException("消息被 broker 退回 (mandatory): " + ret);
            }
        }

        if (meta.getPublisherConfirmType() != RabbitMqProducerOutputMeta.PublisherConfirmType.NONE) {
            ch.waitForConfirmsOrDie(30_000);
        }
    }

    private void closeQuietly() {
        if (data == null) {
            return;
        }
        data.open = false;
        try {
            if (data.channel != null && data.channel.isOpen()) {
                data.channel.close();
            }
        } catch (Exception ignored) {
            // ignore
        }
        try {
            if (data.connection != null && data.connection.isOpen()) {
                data.connection.close();
            }
        } catch (Exception ignored) {
            // ignore
        }
        data.channel = null;
        data.connection = null;
    }

    @Override
    public void stopRunning(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
        closeQuietly();
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        closeQuietly();
        super.dispose(smi, sdi);
    }
}
