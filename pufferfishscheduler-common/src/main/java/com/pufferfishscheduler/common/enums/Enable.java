package com.pufferfishscheduler.common.enums;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

/**
 * 启用状态
 * @author Mayc
 * @since 2025-11-25  20:24
 */
@Getter
public enum Enable {
    ENABLE("0", "启用"),

    OSS("1", "禁用");

    /**
     * -- GETTER --
     *  获取状态码
     *
     * @return String 状态码
     */
    private final String code;
    /**
     * -- GETTER --
     *  获取状态描述
     *
     * @return String 状态描述
     */
    private final String description;

    /**
     * 私有构造函数
     *
     * @param code        状态码 (字符串类型)
     * @param description 状态描述
     */
    Enable(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * 根据状态码获取对应的 Enable 枚举实例 (推荐使用)。
     * <p>
     * 这是一个类型安全的转换方法，参数和枚举的 code 字段都是 String 类型。
     * 当输入未知的状态码时，它会返回一个空的 {@link Optional} 对象，避免了空指针异常。
     *
     * @param code 状态码 (字符串类型)
     * @return 一个包含对应 {@link Enable} 实例的 {@link Optional}，如果找不到则为空
     */
    public static Optional<Enable> fromCode(String code) {
        return Arrays.stream(Enable.values())
                .filter(status -> status.getCode().equals(code))
                .findFirst();
    }

    /**
     * 根据状态码获取对应的 Enable 枚举实例 (快速失败版)。
     * <p>
     * 适用于调用方能够保证输入状态码（字符串类型）一定有效的场景。
     * 如果输入了无效的状态码，它会立即抛出 {@link IllegalArgumentException}。
     *
     * @param code 状态码 (字符串类型)
     * @return TaskStatus 枚举实例
     * @throws IllegalArgumentException 如果状态码无效
     */
    public static Enable valueOfCode(String code) {
        return fromCode(code)
                .orElseThrow(() -> new IllegalArgumentException("无效的状态类型: " + code));
    }
}
