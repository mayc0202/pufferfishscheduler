package com.pufferfishscheduler.common.enums;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

/**
 * 任务状态枚举
 *
 * @author Mayc
 * @since 2025-11-25  15:41
 */
@Getter
public enum TaskStatus {

    /**
     * "0": 未启动 - 任务已创建但尚未开始执行。
     */
    NOT_STARTED("0", "未启动"),

    /**
     * "1": 启动中 - 任务正在从 '未启动' 状态过渡到 '运行中' 状态。
     */
    STARTING("1", "启动中"),

    /**
     * "2": 运行中 - 任务正在正常执行其核心逻辑。
     */
    RUNNING("2", "运行中"),

    /**
     * "3": 成功 - 任务已完成所有执行步骤且没有错误。
     */
    SUCCESS("3", "成功"),

    /**
     * "4": 失败 - 任务在执行过程中遇到不可恢复的错误而终止。
     */
    FAILED("4", "失败"),

    /**
     * "5": 停止中 - 任务正在从 '运行中' 状态过渡到 '已停止' 状态。
     */
    STOPPING("5", "停止中"),

    /**
     * "6": 已停止 - 任务在完成前被用户或系统主动停止。
     */
    STOPPED("6", "已停止"),

    /**
     * "7": 异常 - 任务处于一种未知的或需要特殊处理的错误状态。
     */
    EXCEPTION("7", "异常");

    /**
     * -- GETTER --
     * 获取状态码
     *
     * @return String 状态码
     */
    private final String code;
    /**
     * -- GETTER --
     * 获取状态描述
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
    TaskStatus(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * 根据状态码获取对应的 TaskStatus 枚举实例 (推荐使用)。
     * <p>
     * 这是一个类型安全的转换方法，参数和枚举的 code 字段都是 String 类型。
     * 当输入未知的状态码时，它会返回一个空的 {@link Optional} 对象，避免了空指针异常。
     *
     * @param code 状态码 (字符串类型)
     * @return 一个包含对应 {@link TaskStatus} 实例的 {@link Optional}，如果找不到则为空
     */
    public static Optional<TaskStatus> fromCode(String code) {
        return Arrays.stream(TaskStatus.values())
                .filter(status -> status.getCode().equals(code))
                .findFirst();
    }

    /**
     * 根据状态码获取对应的 TaskStatus 枚举实例 (快速失败版)。
     * <p>
     * 适用于调用方能够保证输入状态码（字符串类型）一定有效的场景。
     * 如果输入了无效的状态码，它会立即抛出 {@link IllegalArgumentException}。
     *
     * @param code 状态码 (字符串类型)
     * @return TaskStatus 枚举实例
     * @throws IllegalArgumentException 如果状态码无效
     */
    public static TaskStatus valueOfCode(String code) {
        return fromCode(code)
                .orElseThrow(() -> new IllegalArgumentException("无效的任务状态码: " + code));
    }
}
