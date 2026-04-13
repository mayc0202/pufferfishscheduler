package com.pufferfishscheduler.common.config.file;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 文件路径配置（Master / Worker 共用）
 */
@Data
@Component
public class FilePathConfig {

    @Value("${file.path.ftp}")
    private String ftpPath;

    @Value("${file.path.local}")
    private String localPath;

    /**
     * 输入文件根路径；FTP Excel 等按流程缓存为 {@code inputPath/{flowId}/文件名}
     */
    @Value("${file.path.input}")
    private String inputPath;

    @Value("${file.path.output}")
    private String outputPath;
}
