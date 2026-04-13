package com.pufferfishscheduler.plugin;

import com.rabbitmq.client.ConnectionFactory;
import java.util.Map;
import java.util.function.Function;

public final class RabbitMqConnectionSupport {

    private RabbitMqConnectionSupport() {}

    public static ConnectionFactory buildFactory(
            String host,
            String port,
            String username,
            String password,
            String virtualHost,
            Map<String, String> config,
            Function<String, String> subst) {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(subst.apply(host));
        int p = 5672;
        try {
            p = Integer.parseInt(subst.apply(port != null ? port : "5672"));
        } catch (NumberFormatException ignored) {
            // keep default
        }
        f.setPort(p);
        f.setUsername(subst.apply(username));
        f.setPassword(subst.apply(password));
        String vh = subst.apply(virtualHost);
        if (vh == null || vh.isEmpty()) {
            vh = "/";
        }
        f.setVirtualHost(vh);

        if (config != null) {
            applyInt(config, subst, "connectionTimeout", f::setConnectionTimeout);
            applyInt(config, subst, "handshakeTimeout", f::setHandshakeTimeout);
            String uri = subst.apply(config.get("uri"));
            if (uri != null && !uri.isEmpty()) {
                try {
                    f.setUri(uri);
                } catch (Exception ignored) {
                    // keep host/port settings
                }
            }
        }
        return f;
    }

    private static void applyInt(
            Map<String, String> config,
            Function<String, String> subst,
            String key,
            java.util.function.IntConsumer setter) {
        String v = config.get(key);
        if (v == null || v.isEmpty()) {
            return;
        }
        try {
            setter.accept(Integer.parseInt(subst.apply(v)));
        } catch (NumberFormatException ignored) {
            // skip
        }
    }
}
