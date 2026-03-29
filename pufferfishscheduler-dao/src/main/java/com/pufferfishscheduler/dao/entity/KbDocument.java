package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

/**
 * 知识库主表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "kb_document")
public class KbDocument implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 知识ID(UUID)
     */
    @TableId
    private String id;

    /**
     * 标题
     */
    @TableField(value = "title")
    private String title;

    /**
     * 分类ID
     */
    @TableField(value = "category_id")
    private Integer categoryId;

    /**
     * 状态 0-禁用 1-启用
     */
    @TableField(value = "status")
    private Integer status;

    /**
     * 正文内容
     */
    @TableField(value = "content")
    private String content;

    /**
     * 标签JSON数组
     */
    @TableField(value = "tags_json")
    private String tagsJson;

    /**
     * 是否有附件
     */
    @TableField(value = "has_attachment")
    private Integer hasAttachment;

    /**
     * 切片状态 0未切片 1切片中 2成功 3失败
     */
    @TableField(value = "chunk_status")
    private Integer chunkStatus;

    /**
     * 切片数量
     */
    @TableField(value = "chunk_count")
    private Integer chunkCount;

    /**
     * 向量命名空间
     */
    @TableField(value = "vector_namespace")
    private String vectorNamespace;

    /**
     * 删除状态 0未删除 1已删除
     */
    @TableField(value = "deleted")
    private Boolean deleted;

    /**
     * 创建人
     */
    @TableField(value = "created_by")
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField(value = "created_time")
    private Date createdTime;

    /**
     * 更新人
     */
    @TableField(value = "updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
    @TableField(value = "updated_time")
    private Date updatedTime;
}
