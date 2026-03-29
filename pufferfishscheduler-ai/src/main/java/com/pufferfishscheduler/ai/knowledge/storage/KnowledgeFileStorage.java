package com.pufferfishscheduler.ai.knowledge.storage;

import org.springframework.web.multipart.MultipartFile;

/**
 * 知识库文件存储抽象
 */
public interface KnowledgeFileStorage {

    /**
     * 上传附件
     *
     * @param file 文件
     * @param relativeDir 相对目录
     * @return 远程完整路径
     */
    String upload(MultipartFile file, String relativeDir);

    /**
     * 删除附件
     *
     * @param remotePath 远程路径
     */
    void delete(String remotePath);
}
