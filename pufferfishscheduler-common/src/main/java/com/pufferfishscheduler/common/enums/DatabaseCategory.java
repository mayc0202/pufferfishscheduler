package com.pufferfishscheduler.common.enums;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

/**
 * 数据库大类：1-关系型数据库；2-非关系型数据库；3-消息型数据库；4-FTP类型; 5-OSS
 *
 * @author Mayc
 * @since 2025-11-25  16:04
 */
@Getter
public enum DatabaseCategory {

    SQL("1", "关系型数据库"),

    NO_SQL("2", "非关系型数据库"),

    MQ("3", "消息型数据库"),

    FTP("4", "FTP类型"),

    OSS("5", "OSS");

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
    DatabaseCategory(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * 根据状态码获取对应的 DatabaseCategory 枚举实例 (推荐使用)。
     * <p>
     * 这是一个类型安全的转换方法，参数和枚举的 code 字段都是 String 类型。
     * 当输入未知的状态码时，它会返回一个空的 {@link Optional} 对象，避免了空指针异常。
     *
     * @param code 状态码 (字符串类型)
     * @return 一个包含对应 {@link DatabaseCategory} 实例的 {@link Optional}，如果找不到则为空
     */
    public static Optional<DatabaseCategory> fromCode(String code) {
        return Arrays.stream(DatabaseCategory.values())
                .filter(status -> status.getCode().equals(code))
                .findFirst();
    }

    /**
     * 根据状态码获取对应的 DatabaseCategory 枚举实例 (快速失败版)。
     * <p>
     * 适用于调用方能够保证输入状态码（字符串类型）一定有效的场景。
     * 如果输入了无效的状态码，它会立即抛出 {@link IllegalArgumentException}。
     *
     * @param code 状态码 (字符串类型)
     * @return TaskStatus 枚举实例
     * @throws IllegalArgumentException 如果状态码无效
     */
    public static DatabaseCategory valueOfCode(String code) {
        return fromCode(code)
                .orElseThrow(() -> new IllegalArgumentException("无效的数据库类型: " + code));
    }
}
