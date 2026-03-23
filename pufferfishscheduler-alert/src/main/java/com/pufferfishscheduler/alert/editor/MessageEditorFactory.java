package com.pufferfishscheduler.alert.editor;

import com.pufferfishscheduler.alert.editor.editor.EmailMessageEditor;
import com.pufferfishscheduler.alert.editor.editor.SmsMessageEditor;
import com.pufferfishscheduler.common.exception.BusinessException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息编辑器工厂
 *
 * @author Mayc
 * @since 2026-03-23  14:06
 */
@Component
public class MessageEditorFactory {

    private static final String METHOD_EMAIL = "EMAIL";
    private static final String METHOD_SMS = "SMS";

    /**
     * 预警方式到编辑器的映射
     */
    private final Map<String, AbstractMessageEditor> editorMap;

    public MessageEditorFactory(EmailMessageEditor emailMessageEditor, SmsMessageEditor smsMessageEditor) {
        Map<String, AbstractMessageEditor> map = new ConcurrentHashMap<>();
        map.put(METHOD_EMAIL, emailMessageEditor);
        map.put(METHOD_SMS, smsMessageEditor);
        this.editorMap = Collections.unmodifiableMap(map);
    }

    /**
     * 根据预警方式获取编辑器
     *
     * @param alertMethod 预警方式
     * @return 对应的预警编辑器
     */
    public AbstractMessageEditor getMessageEditor(String alertMethod) {
        if (StringUtils.isBlank(alertMethod)) {
            throw new BusinessException("预警方式不能为空！");
        }

        String normalizedMethod = alertMethod.trim().toUpperCase();
        AbstractMessageEditor editor = editorMap.get(normalizedMethod);
        if (editor == null) {
            throw new BusinessException("不支持的预警方式：" + alertMethod + "，仅支持：SMS、EMAIL");
        }
        return editor;
    }
}
