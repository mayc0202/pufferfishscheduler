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
 * 知识库切片索引表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "kb_document_chunk")
public class KbDocumentChunk implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 切片ID(UUID)
     */
    @TableId
    private String id;

    /**
     * 知识ID
     */
    @TableField(value = "document_id")
    private String documentId;

    /**
     * 附件ID，正文切片可空
     */
    @TableField(value = "attachment_id")
    private String attachmentId;

    /**
     * 切片序号
     */
    @TableField(value = "chunk_no")
    private Integer chunkNo;

    /**
     * 向量库记录ID
     */
    @TableField(value = "vector_id")
    private String vectorId;

    /**
     * 切片元数据
     */
    @TableField(value = "metadata_json")
    private String metadataJson;

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
