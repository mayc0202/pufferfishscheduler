package com.pufferfishscheduler.master.upms.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.UserContact;
import com.pufferfishscheduler.domain.form.user.UserContactForm;
import com.pufferfishscheduler.domain.vo.user.UserContactVo;

/**
 * 用户中心-联系人（管理员）
 */
public interface UserContactService extends IService<UserContact> {

    /**
     * 查询联系人分页列表
     *
     * @param name 姓名
     * @param pageNo 页码
     * @param pageSize 每页数量
     * @return 联系人分页列表VO
     */
    IPage<UserContactVo> page(String name, Integer pageNo, Integer pageSize);

    /**
     * 查询联系人详情
     *
     * @param id 联系人ID
     * @return 联系人详情VO
     */
    UserContactVo detail(Integer id);

    /**
     * 新增联系人
     *
     * @param form 联系人表单
     */
    void add(UserContactForm form);

    /**
     * 更新联系人
     *
     * @param form 联系人表单
     */
    void update(UserContactForm form);

    /**
     * 删除联系人
     *
     * @param id 联系人ID
     */
    void delete(Integer id);
}
