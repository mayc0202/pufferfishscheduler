package com.pufferfishscheduler.ai.agent.service;

import org.springframework.web.multipart.MultipartFile;

/**
 *
 * @author Mayc
 * @since 2026-02-22  12:49
 */
public interface KnowledgeBaseService {

    /**
     * 文件切片
     * @param file
     */
    void fileChunk(MultipartFile file);
}
