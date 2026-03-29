package com.pufferfishscheduler.plugin.common.enums;

public enum DesensitizationType {
    PAPERSNo("身份证脱敏"),
    CARDNO("证件号码脱敏"),
    PHONENO("手机号码脱敏"),
    EMAILNO("邮箱脱敏"),
    CARPLATENO("车牌号脱敏"),
    ADDRESS("地址信息脱敏"),
    MD5("MD5加密");

    private String description;
    
    DesensitizationType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }
}
