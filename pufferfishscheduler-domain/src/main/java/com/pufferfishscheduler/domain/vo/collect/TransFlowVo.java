package com.pufferfishscheduler.domain.vo.collect;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.util.Date;

/**
 *
 * @author Mayc
 * @since 2026-03-05  11:52
 */
@Data
public class TransFlowVo {

    private Integer id;

    /**
     * 流程名称
     */
    private String name;

    /**
     * 描述
     */
    private String description;

    /**
     * 配置
     */
    private String config;

    /**
     * 路径
     */
    private String path;

    /**
     * 数据处理阶段
     */
    private String stage;

    /**
     * 分类id
     */
    private Integer groupId;

    /**
     * 分类名称
     */
    private String groupName;

    /**
     * 行大小
     */
    private Integer rowSize;

    /**
     * 是否删除，0-否；1-是
     */
    private Boolean deleted;

    /**
     * 创建人账号
     */
    private String createdBy;

    /**
     * 创建时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date createdTime;

    private String createdTimeTxt;

    /**
     * 更新人账号
     */
    private String updatedBy;

    /**
     * 更新时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date updatedTime;
}
