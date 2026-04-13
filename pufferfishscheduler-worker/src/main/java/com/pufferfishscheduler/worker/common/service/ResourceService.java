package com.pufferfishscheduler.worker.common.service;

import java.util.List;

/**
 * 资源管理 Service
 */
public interface ResourceService {

    /**
     * 上传文件并指定文件名
     *
     * @param dbId      数据源Id
     * @param path      上传目录
     * @param fileNames 文件名列表
     */
    void uploadWithNames(Integer dbId, String path, List<String> fileNames);


    /**
     * 删除本地临时目录及其所有内容
     *
     * @param localPath 本地目录绝对路径
     */
    void deleteLocalDirectory(String localPath);

    /**
     * 同 {@link # downloadFileToLocal(Integer, String, String, String, String, String, boolean)}；
     * 当 {@code trustLocalCacheWithoutFtp} 为 true 且本地已存在有效 Excel 缓存时，不再连接 FTP（用于设计器连续调用 getSheets/getFields）。
     */
    void downloadFileToLocal(
            Integer dbId,
            String remoteRelativeDir,
            String fileName,
            String localFullPath,
            boolean trustLocalCacheWithoutFtp);
}
