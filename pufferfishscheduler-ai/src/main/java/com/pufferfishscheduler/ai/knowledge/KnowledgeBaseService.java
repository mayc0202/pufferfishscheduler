package com.pufferfishscheduler.ai.knowledge;

import com.pufferfishscheduler.ai.knowledge.model.KnowledgeCreateCommand;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

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

    /**
     * 创建知识并处理附件切片
     *
     * @param command 知识参数
     * @param files 附件
     * @return 知识ID
     */
    String createKnowledge(KnowledgeCreateCommand command, List<MultipartFile> files);

    /**
     * 删除知识（含FTP附件和向量数据）
     *
     * @param documentId 知识ID
     */
    void deleteKnowledge(String documentId);
}
