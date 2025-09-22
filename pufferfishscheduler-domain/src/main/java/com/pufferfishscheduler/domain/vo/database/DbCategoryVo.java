package com.pufferfishscheduler.domain.vo.database;

import com.alibaba.fastjson2.annotation.JSONField;
import com.pufferfishscheduler.domain.DomainConstants;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Author: yc
 * @CreateTime: 2025-05-21
 * @Description:
 * @Version: 1.0
 */
@Data
public class DbCategoryVo {

    private Integer id;

    /**
     * 数据库名称
     */
    private String name;

    /**
     * logo图片
     */
    private String img;

    /**
     * 图片配置
     */
    private String imgConfig;

    /**
     * 排序
     */
    private Integer orderBy;

    /**
     * 创建人
     */
    private String createdBy;

    /**
     * 创建时间
     */
    @JSONField(format = DomainConstants.DEFAULT_DATE_TIME_FORMAT)
    private Date createdTime;

    /**
     * 更新人
     */
    private String updatedBy;

    /**
     * 更新时间
     */
    @JSONField(format = DomainConstants.DEFAULT_DATE_TIME_FORMAT)
    private Date updatedTime;
}