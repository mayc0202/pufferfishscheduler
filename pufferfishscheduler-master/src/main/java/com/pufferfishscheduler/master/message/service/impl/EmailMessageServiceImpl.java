package com.pufferfishscheduler.master.message.service.impl;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.master.message.service.EmailMessageService;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * 邮件消息服务实现（使用 application.yml 中 spring.mail 配置）
 *
 * @author Mayc
 * @since 2026-03-20
 */
@Slf4j
@Service
public class EmailMessageServiceImpl implements EmailMessageService {

    @Autowired
    private JavaMailSender javaMailSender;

    @Value("${spring.mail.username}")
    private String fromEmail;

    @Override
    public void sendEmail(String recipientName, String recipientEmail, String subject, String content) {
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
