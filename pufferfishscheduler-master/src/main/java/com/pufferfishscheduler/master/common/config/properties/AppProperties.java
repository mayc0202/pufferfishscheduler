package com.pufferfishscheduler.master.common.config.properties;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 应用配置属性
 *
 * @author Mayc
 * @since 2026-03-17  15:53
 */
@Data
@Configuration
public class AppProperties {

    /**
     * 是否为开发模式，默认false
     */
    @Value("${app.dev-mode.enabled}")
    private Boolean devModeEnabled;

    /**
     * 是否发送预警邮件，默认false
     */
    @Value("${app.send-alert-email.enabled}")
    private Boolean sendAlertEmailEnabled;

}
