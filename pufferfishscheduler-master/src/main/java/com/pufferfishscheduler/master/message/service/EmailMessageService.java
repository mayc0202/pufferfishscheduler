package com.pufferfishscheduler.master.message.service;

/**
 * 邮件消息服务
 *
 * @author Mayc
 * @since 2026-03-20
 */
public interface EmailMessageService {

    /**
     * 发送邮件消息
     *
     * @param recipientName   收件人
     * @param recipientEmail  收件人邮箱
     * @param subject         邮件标题
     * @param content         邮件内容（支持 HTML 片段）
     */
    void sendEmail(String recipientName, String recipientEmail, String subject, String content);
}
