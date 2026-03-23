package com.pufferfishscheduler.domain.vo.user;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * 用户中心-联系人（列表/详情）
 */
@Data
public class UserContactVo {

    private Long id;
    private String name;
    private String phone;
    private String email;
    /**
     * 预警方式（多选解析后的列表）
     */
    private List<String> alertMethods;
    private String remark;
    private String createdBy;
    private Date createdTime;
    private String updatedBy;
    private Date updatedTime;
}
