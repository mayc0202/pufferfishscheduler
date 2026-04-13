package com.pufferfishscheduler.domain.vo.collect;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文件VO
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FileVo {

    /**
     * 文件名
     */
    private String name;

    /**
     * 文件掩码
     */
    private String fileMask;

    /**
     * 排除文件掩码
     */
    private String excludeMask;

    /**
     * 文件路径
     */
    private String path;

    /**
     * 文件类型
     */
    private String type;
}
