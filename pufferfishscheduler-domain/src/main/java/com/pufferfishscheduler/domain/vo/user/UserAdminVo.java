package com.pufferfishscheduler.domain.vo.user;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * 用户管理列表/详情（不含密码）
 */
@Data
public class UserAdminVo {

    private Integer id;
    private String avatar;
    private String account;
    private String phone;
    private String email;
    private String name;
    private String wechat;
    private Integer deptId;
    private Date expireDate;
    private String createdBy;
    private Date createdTime;
    private String updatedBy;
    private Date updatedTime;

    private List<Integer> roleIds;
    private List<String> roleNames;
}
