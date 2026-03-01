package com.pufferfishscheduler.ai.knowledge.controller;

import com.pufferfishscheduler.ai.knowledge.service.KnowledgeBaseService;
import com.pufferfishscheduler.common.result.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * Knowledge Base Controller
 * @author Mayc
 * @since 2026-02-22  12:47
 */
@RestController
@RequestMapping("/knowledge")
public class KnowledgeBaseController {

    @Autowired
    private KnowledgeBaseService knowledgeBaseService;

    @PostMapping("/fileChunk")
    public ApiResponse fileChunk(@RequestParam("file") MultipartFile file) {
        knowledgeBaseService.fileChunk(file);
        return ApiResponse.success("文件切片成功!");
    }

}
