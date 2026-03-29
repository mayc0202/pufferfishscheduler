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
 * 知识库附件表
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "kb_document_attachment")
public class KbDocumentAttachment implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 附件ID(UUID)
     */
    @TableId
    private String id;

    /**
     * 知识ID
     */
    @TableField(value = "document_id")
    private String documentId;

    /**
     * 原始文件名
     */
    @TableField(value = "file_name")
    private String fileName;

    /**
     * 扩展名
     */
    @TableField(value = "file_ext")
    private String fileExt;

    /**
     * MIME类型
     */
    @TableField(value = "mime_type")
    private String mimeType;

    /**
     * 文件大小(byte)
     */
    @TableField(value = "file_size")
    private Long fileSize;

    /**
     * MD5
     */
    @TableField(value = "file_md5")
    private String fileMd5;

    /**
     * FTP相对路径
     */
    @TableField(value = "ftp_path")
    private String ftpPath;

    /**
     * 解析状态 0未解析 1解析中 2成功 3失败
     */
    @TableField(value = "parse_status")
    private Integer parseStatus;

    /**
     * 删除状态 未删除 已删除
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
