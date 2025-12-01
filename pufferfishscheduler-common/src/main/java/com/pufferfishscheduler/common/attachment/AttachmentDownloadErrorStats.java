package com.pufferfishscheduler.common.attachment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 附件下载错误状态记录器
 *
 * @author Mayc
 * @since 2025-08-29  14:43
 */
public class AttachmentDownloadErrorStats {
    private static final ConcurrentHashMap<String, List<String>> ERROR_STATS = new ConcurrentHashMap<>();

    /**
     * 记录下载错误
     *
     * @param fileName     文件名称
     * @param errorMessage 错误信息
     */
    public static void recordError(String fileName, String errorMessage) {
        ERROR_STATS.compute(fileName, (key, existingList) -> {
            if (existingList == null) {
                existingList = new ArrayList<>();
            }
            existingList.add(System.currentTimeMillis() + " - " + errorMessage);
            return existingList;
        });
    }

    /**
     * 获取指定文件错误记录
     *
     * @param fileName 文件名称
     * @return 错误列表
     */
    public static List<String> getErrors(String fileName) {
        return ERROR_STATS.getOrDefault(fileName, new ArrayList<>());
    }

    /**
     * 清除指定文件的错误记录
     *
     * @param fileName 文件名称
     */
    public static void clearErrors(String fileName) {
        ERROR_STATS.remove(fileName);
    }

    /**
     * 生成错误报告内容
     *
     * @param fileName     文件名称
     * @return 错误报告文本
     */
    public static String generateErrorReport(String fileName) {
        List<String> errors = getErrors(fileName);
        if (errors.isEmpty()) {
            return null;
        }

        StringBuilder report = new StringBuilder();
        report.append("下载文件错误报告\n");
        report.append("====================\n");
        report.append("名称: ").append(fileName).append("\n");
        report.append("生成时间: ").append(new java.util.Date()).append("\n");
        report.append("错误数量: ").append(errors.size()).append("\n");
        report.append("====================\n\n");

        for (int i = 0; i < errors.size(); i++) {
            report.append("错误 ").append(i + 1).append(":\n");
            report.append("  ").append(errors.get(i)).append("\n\n");
        }

        report.append("====================\n");
        report.append("报告结束\n");

        return report.toString();
    }
}
