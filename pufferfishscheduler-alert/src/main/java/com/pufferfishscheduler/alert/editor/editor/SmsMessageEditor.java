package com.pufferfishscheduler.alert.editor.editor;

import com.alibaba.fastjson2.JSONObject;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.pufferfishscheduler.alert.config.PhoneCodeProperties;
import com.pufferfishscheduler.alert.editor.AbstractMessageEditor;
import com.pufferfishscheduler.common.exception.BusinessException;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 短信消息编辑器
 *
 * @author Mayc
 * @since 2026-03-23  13:51
 */
@Slf4j
@Component
public class SmsMessageEditor extends AbstractMessageEditor {

    @Resource
    private PhoneCodeProperties properties;

    /**
     * 发送短信通知
     *
     * @param recipientName  接收人姓名
     * @param recipientPhone 接收地址（手机号）
     * @param subject        主题/标题
     * @param content        内容
     */
    @Override
    public void sendNotification(String recipientName, String recipientPhone, String subject, String content) {
        if (StringUtils.isBlank(recipientPhone)) {
            throw new BusinessException("收件人手机号不能为空!");
        }
        if (StringUtils.isBlank(content)) {
            throw new BusinessException("短信内容不能为空!");
        }
        if (StringUtils.isBlank(properties.getAccessKey()) || StringUtils.isBlank(properties.getSecret())) {
            throw new BusinessException("短信服务密钥配置不完整!");
        }
        if (StringUtils.isBlank(properties.getSignName()) || StringUtils.isBlank(properties.getTemplateCode())) {
            throw new BusinessException("短信服务签名或模板配置不能为空!");
        }
        if (StringUtils.isBlank(properties.getDomain()) || StringUtils.isBlank(properties.getApiversion())
                || StringUtils.isBlank(properties.getAction())) {
            throw new BusinessException("短信服务基础配置不完整!");
        }

        Map<String, Object> map = new HashMap<>();
        map.put("message", content);
        // 设置配置文件                                                                   密钥id                       密钥
        DefaultProfile profile = DefaultProfile.getProfile("default", properties.getAccessKey(), properties.getSecret());
        IAcsClient client = new DefaultAcsClient(profile); // 创建一个阿里云授权的终端
        CommonRequest request = new CommonRequest(); // 创建请求
        request.setSysMethod(MethodType.POST); // post方法
        request.setSysDomain(properties.getDomain());
        request.setSysVersion(properties.getApiversion()); // API版本号
        request.setSysAction(properties.getAction());
        request.putQueryParameter("PhoneNumbers", recipientPhone); // 手机号
        request.putQueryParameter("SignName", properties.getSignName()); // 签名名称
        request.putQueryParameter("TemplateCode", properties.getTemplateCode()); // 短信模板
        request.putQueryParameter("TemplateParam", JSONObject.toJSONString(map)); // 验证码转成json数据
        try {
            CommonResponse response = client.getCommonResponse(request);
            JSONObject json = JSONObject.parseObject(response.getData());
            String message = json.getString("Message");
            String code = json.getString("Code");
            if ("只能向已回复授权信息的手机号发送".equals(message)) {
                throw new BusinessException("只能向已回复授权信息的手机号发送");
            }
            if (!"OK".equalsIgnoreCase(code)) {
                throw new BusinessException("短信发送失败：" + StringUtils.defaultIfBlank(message, code));
            }
            log.debug("发送短信状态：{}", response.getHttpResponse().getStatus());

        } catch (Exception e) {
            log.error("短信发送异常：{}", e.getMessage());
            throw new BusinessException(String.format("短信发送异常：%s", e.getMessage()));
        }
    }
}
