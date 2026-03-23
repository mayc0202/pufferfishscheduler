package com.pufferfishscheduler.alert.editor;

/**
 * 抽象消息编辑器
 *
 * @author Mayc
 * @since 2026-03-23  13:49
 */
public abstract class AbstractMessageEditor {

    /**
     * 发送通知
     *
     * @param recipientName 接收人姓名
     * @param recipient     接收地址（邮箱/手机号）
     * @param subject       主题/标题
     * @param content       内容
     */
    public abstract void sendNotification(String recipientName, String recipient, String subject, String content);

}
