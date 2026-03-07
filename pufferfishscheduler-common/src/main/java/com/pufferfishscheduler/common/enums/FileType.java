package com.pufferfishscheduler.common.enums;

import com.pufferfishscheduler.common.exception.BusinessException;
import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

/**
 *
 * @author Mayc
 * @since 2026-02-22  13:12
 */
@Getter
public enum FileType {
    CSV("csv", ""),
    XLSX("xlsx", ""),
    XLS("xls", ""),
    PSD("pdf", ""),
    DOCX("docx", ""),
    DOC("doc", ""),
    TXT("txt", ""),
    MD("md", "");

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
    FileType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     *
     * @param code
     * @return
     */
    public static Optional<FileType> fromCode(String code) {
        return Arrays.stream(FileType.values())
                .filter(status -> status.getCode().equals(code))
                .findFirst();
    }

    /**
     *
     * @param code
     * @return
     */
    public static FileType valueOfCode(String code) {
        return fromCode(code)
                .orElseThrow(() -> new BusinessException("无效的状态类型: " + code));
    }
}
