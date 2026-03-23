package com.pufferfishscheduler.alert.controller;

import com.pufferfishscheduler.alert.service.AlertService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 告警控制器
 *
 * @author Mayc
 * @since 2026-03-22  20:28
 */
@RestController
@RequestMapping("/alert")
public class AlterController {

    @Autowired
    private AlertService alertService;

    /**
     * 发送预警通知
     *
     * @param alertMethod    预警方式
     * @param recipientName  接收人姓名
     * @param recipientPhone 接收人手机号
     * @param recipientPhone 接收人手机号
     * @param subject        邮件主题
     * @param content        邮件内容
     */
    @PostMapping("/sendNotification")
    public void sendNotification(String alertMethod, String recipientName, String recipientPhone, String subject, String content) {
        alertService.sendNotification(alertMethod, recipientName, recipientPhone, subject, content);
    }

}
