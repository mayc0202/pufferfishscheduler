package com.pufferfishscheduler.domain.vo.database;

import com.alibaba.fastjson2.annotation.JSONField;
import com.pufferfishscheduler.domain.DomainConstants;
import lombok.Data;

import java.util.Date;

@Data
public class ResourceVo {

    /**
     * 数据源id
     */
    private Integer dbId;

    /**
     * 文件/文件夹名称
     */
    private String name;

    /**
     * 资源类型:1-文件夹；2-文件
     */
    private String type;

    /**
     * 资源大小
     */
    private String size;

    /**
     * 创建时间
     */
    @JSONField(format = DomainConstants.DEFAULT_DATE_TIME_FORMAT)
    private Date createdTime;

    private String createdTimeTxt;

}
