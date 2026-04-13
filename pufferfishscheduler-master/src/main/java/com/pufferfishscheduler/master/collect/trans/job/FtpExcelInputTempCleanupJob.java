package com.pufferfishscheduler.master.collect.trans.job;

import com.pufferfishscheduler.master.common.config.file.FilePathConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

/**
 * 清理 FTP Excel 输入在本地按流程 ID 缓存的临时文件（{@code file.path.input/{flowId}/*}），避免长期占盘。
 */
@Slf4j
@Component
public class FtpExcelInputTempCleanupJob {

    /**
     * 超过该时间未修改的缓存文件将被删除（目录保留）
     */
    private static final long CACHE_FILE_TTL_MS = 72L * 3600 * 1000;

    @Autowired
    private FilePathConfig filePathConfig;

    /**
     * 每天凌晨 2 点执行
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void cleanupStaleFiles() {
        if (filePathConfig == null || StringUtils.isBlank(filePathConfig.getInputPath())) {
            return;
        }
        File base = new File(filePathConfig.getInputPath());
        if (!base.isDirectory()) {
            return;
        }
        File[] flowDirs = base.listFiles(File::isDirectory);
        if (flowDirs == null) {
            return;
        }
        long cutoff = System.currentTimeMillis() - CACHE_FILE_TTL_MS;
        int removed = 0;
        for (File dir : flowDirs) {
            File[] entries = dir.listFiles();
            if (entries == null) {
                continue;
            }
            for (File f : entries) {
                if (f.isDirectory() && f.getName().startsWith("xl-")) {
                    try {
                        FileUtils.deleteDirectory(f);
                        log.info("已移除历史 xl-* 缓存目录: {}", f.getAbsolutePath());
                    } catch (IOException e) {
                        log.warn("删除历史 xl-* 目录失败: {}", f.getAbsolutePath(), e);
                    }
                    continue;
                }
                if (f.isFile() && f.lastModified() < cutoff) {
                    if (f.delete()) {
                        removed++;
                    } else {
                        log.warn("定时清理未能删除过期文件: {}", f.getAbsolutePath());
                    }
                }
            }
        }
        if (removed > 0) {
            log.info("FTP Excel 输入临时文件定时清理完成，共删除 {} 个文件", removed);
        }
    }
}
