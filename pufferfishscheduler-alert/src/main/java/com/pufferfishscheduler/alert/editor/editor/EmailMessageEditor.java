package com.pufferfishscheduler.alert.editor.editor;

import com.pufferfishscheduler.alert.editor.AbstractMessageEditor;
import com.pufferfishscheduler.common.exception.BusinessException;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 *
 * @author Mayc
 * @since 2026-03-23  13:49
 */
@Slf4j
@Component
public class EmailMessageEditor extends AbstractMessageEditor {

    @Autowired
    private JavaMailSender javaMailSender;

    @Value("${spring.mail.username}")
    private String fromEmail;

    /**
     * 发送邮件通知
     *
     * @param recipientName  接收人姓名
     * @param recipientEmail 接收地址（邮箱）
     * @param subject        主题/标题
     * @param content        内容
     */
    @Override
    public void sendNotification(String recipientName, String recipientEmail, String subject, String content) {
        if (StringUtils.isBlank(recipientEmail)) {
            throw new BusinessException("收件人邮箱不能为空!");
        }
        if (StringUtils.isBlank(subject)) {
            throw new BusinessException("邮件标题不能为空!");
        }
        if (StringUtils.isBlank(content)) {
            throw new BusinessException("邮件内容不能为空!");
        }
        if (StringUtils.isBlank(fromEmail)) {
            throw new BusinessException("发件人邮箱配置不能为空!");
        }

        String safeFromEmail = Objects.requireNonNull(fromEmail);
        String safeRecipientEmail = Objects.requireNonNull(recipientEmail);
        String safeSubject = Objects.requireNonNull(subject);
        String safeContent = Objects.requireNonNull(content);

        try {
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");
            helper.setFrom(safeFromEmail);
            helper.setTo(safeRecipientEmail);
            helper.setSubject(safeSubject);
            helper.setText(safeContent, true);
            javaMailSender.send(mimeMessage);
            log.info("邮件发送成功, recipientName={}, recipientEmail={}", recipientName, recipientEmail);
        } catch (Exception e) {
            log.error("邮件发送失败, recipientName={}, recipientEmail={}", recipientName, recipientEmail, e);
            throw new BusinessException("邮件发送失败: " + e.getMessage());
        }
    }
}
