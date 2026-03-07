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
 * 转换组件
 *
 * @author Mayc
 * @since 2026-03-02  22:41
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "trans_component")
public class TransComponent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId
    private Integer id;

    /**
     * 组件code
     */
    @TableField("code")
    private String code;
    /**
     * 组件名称
     */
    @TableField("name")
    private String name;

    /**
     * 组件类型  1.输入 2.输出 3.清洗 4.流程 5.脚本 6.文件传输 7.其它
     */
    @TableField("type")
    private String type;

    /**
     * 组件类全路径
     */
    @TableField("class_name")
    private String className;

    /**
     * 图标
     */
    @TableField("icon")
    private String icon;

    /**
     * 排序
     */
    @TableField("order_by")
    private Integer orderBy;

    /**
     * 配置
     */
    @TableField("config")
    private String config;

    /**
     * 是否启用
     */
    @TableField("enabled")
    private Boolean enabled;

    /**
     * 描述
     */
    @TableField("description")
    private String description;

    /**
     * 创建人账号
     */
    @TableField("created_by")
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField("created_time")
    private Date createdTime;

    /**
     * 更新人账号
     */
    @TableField("updated_by")
    private String updatedBy;

    /**
     * 更新时间
     */
    @TableField("updated_time")
    private Date updatedTime;

    /**
     * 是否支持输入
     */
    @TableField("support_input")
    private Boolean supportInput;

    /**
     * 是否支持错误处理
     */
    @TableField("support_error")
    private Boolean supportError;

    /**
     * 是否支持复制
     */
    @TableField("support_copy")
    private Boolean supportCopy;

    /**
     * 是否支持本地文件
     */
    @TableField("support_local_file")
    private Boolean supportLocalFile;

    /**
     * 组件所在阶段 1.输入 2.清洗 3.输出
     */
    @TableField("stage")
    private String stage;


}
