package com.pufferfishscheduler.alert.service.impl;

import com.pufferfishscheduler.alert.editor.AbstractMessageEditor;
import com.pufferfishscheduler.alert.editor.MessageEditorFactory;
import com.pufferfishscheduler.alert.service.AlertService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 告警服务实现类
 *
 * @author Mayc
 * @since 2026-03-22  20:30
 */
@Slf4j
@Service
public class AlertServiceImpl implements AlertService {

    private final MessageEditorFactory messageEditorFactory;

    public AlertServiceImpl(MessageEditorFactory messageEditorFactory) {
        this.messageEditorFactory = messageEditorFactory;
    }

    /**
     * 发送告警通知
     *
     * @param alertMethod 告警方法（如短信、邮件等）
     * @param recipientName 接收人姓名
     * @param recipientPhone 接收人手机号
     * @param subject       主题
     * @param content       内容
     */
    @Override
    public void sendNotification(String alertMethod, String recipientName, String recipientPhone, String subject, String content) {
        AbstractMessageEditor editor = messageEditorFactory.getMessageEditor(alertMethod);
        editor.sendNotification(recipientName, recipientPhone, subject, content);
    }
}
