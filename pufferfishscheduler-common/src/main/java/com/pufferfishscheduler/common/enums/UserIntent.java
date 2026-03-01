package com.pufferfishscheduler.common.enums;

import lombok.Getter;

/**
 * 用户意图枚举
 *
 * @author Mayc
 * @since 2026-02-24  11:01
 */
@Getter
public enum UserIntent {
    PRODUCT_QUERY("product_query", "产品相关问题"),
    DATA_ASSET_QUERY("data_asset_query", "数据资产相关问题"),
    CHITCHAT("chitchat", "闲聊");

    private final String code;
    private final String description;

    UserIntent(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static UserIntent fromCode(String code) {
        for (UserIntent intent : UserIntent.values()) {
            if (intent.getCode().equals(code)) {
                return intent;
            }
        }
        return CHITCHAT; // 默认返回闲聊
    }
}
