package com.pufferfishscheduler.service.database;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.metadata.MetadataTaskForm;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.domain.form.metadata.MetadataTaskUpdateForm;
import com.pufferfishscheduler.domain.vo.metadata.MetadataTaskVo;
import io.swagger.models.auth.In;

import java.util.List;

/**
 * 元数据业务层
 *
 * @author Mayc
 * @since 2025-11-17  17:30
 */
public interface MetadataService {

    /**
     * 获取分组树形结构
     *
     * @param name
     * @return
     */
    List<Tree> tree(String name);

    /**
     * 元数据同步任务列表
     *
     * @param dbId
     * @param dbName
     * @param groupId
     * @param status
     * @param enable
     * @param pageNo
     * @param pageSize
     * @return
     */
    IPage<MetadataTaskVo> list(Integer dbId, String dbName, Integer groupId, String status, Boolean enable, Integer pageNo, Integer pageSize);

    /**
     * 详情
     *
     * @param id
     * @return
     */
    MetadataTaskVo detail(Integer id);

    /**
     * 新增元数据同步任务
     *
     * @param taskForm
     */
    void add(MetadataTaskForm taskForm);

    /**
     * 编辑元数据同步任务
     *
     * @param taskForm
     */
    void update(MetadataTaskUpdateForm taskForm);

    /**
     * 切换启用状态
     *
     * @param id
     */
    void toggleEnableStatus(Integer id);

    /**
     * 删除同步任务
     *
     * @param id
     */
    void delete(Integer id);

    /**
     * 元数据同步
     *
     * @param dbId
     */
    void metadataSync(Integer dbId);
}
