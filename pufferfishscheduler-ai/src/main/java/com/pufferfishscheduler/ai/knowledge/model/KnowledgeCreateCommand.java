package com.pufferfishscheduler.ai.knowledge.model;

import lombok.Data;

import java.util.List;

/**
 * 新增知识参数
 */
@Data
public class KnowledgeCreateCommand {

    /**
     * 标题
     */
    private String title;

    /**
     * 分类ID
     */
    private Integer categoryId;

    /**
     * 状态 0-禁用 1-启用
     */
    private Integer status;

    /**
     * 内容
     */
    private String content;

    /**
     * 标签
     */
    private List<String> tags;

    /**
     * 创建人
     */
    private String operator;

    /**
     * 向量命名空间
     */
    private String vectorNamespace;
}
