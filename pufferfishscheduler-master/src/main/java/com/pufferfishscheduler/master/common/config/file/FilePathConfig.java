package com.pufferfishscheduler.master.common.config.file;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 文件路径配置
 */
@Data
@Component
public class FilePathConfig {

    /**
     * FTP路径
     */
    @Value("${file.path.ftp}")
    private String ftpPath;

    /**
     * 本地路径
     */
    @Value("${file.path.local}")
    private String localPath;

    /**
     * 输入文件根路径；FTP Excel 等按流程缓存为 {@code inputPath/{flowId}/文件名}
     */
    @Value("${file.path.input}")
    private String inputPath;

    /**
     * 输出文件路径
     */
    @Value("${file.path.output}")
    private String outputPath;
}
