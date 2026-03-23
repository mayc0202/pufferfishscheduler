package com.pufferfishscheduler.alert.service;

/**
 * 告警服务接口
 *
 * @author Mayc
 * @since 2026-03-22  20:30
 */
public interface AlertService {

    /**
     * 发送告警通知
     *
     * @param alertMethod 告警方法（如短信、邮件等）
     * @param recipientName 接收人姓名
     * @param recipientPhone 接收人手机号
     * @param subject       主题
     * @param content       内容
     */
    void sendNotification(String alertMethod, String recipientName, String recipientPhone, String subject, String content);
}
