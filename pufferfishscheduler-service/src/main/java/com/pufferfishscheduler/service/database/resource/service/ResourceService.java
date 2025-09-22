package com.pufferfishscheduler.service.database.resource.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.pufferfishscheduler.domain.form.database.ResourceForm;
import com.pufferfishscheduler.domain.vo.TreeVo;
import com.pufferfishscheduler.domain.vo.database.ResourceTreeVo;
import com.pufferfishscheduler.domain.vo.database.ResourceVo;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * 资源管理 Service
 */
public interface ResourceService {

    /**
     * 树形结构
     *
     * @param name
     * @return
     */
    List<TreeVo> tree(String name);

    /**
     * 分页获取资源列表
     *
     * @param dbId
     * @param name
     * @param path
     * @param pageNo
     * @param pageSize
     * @return
     */
    IPage<ResourceVo> list(Integer dbId, String name, String path, Integer pageNo, Integer pageSize);

    /**
     * 上传文件
     *
     * @param dbId
     * @param path
     * @param files
     */
    void upload(Integer dbId, String path, List<MultipartFile> files);

    /**
     * 创建文件夹
     *
     * @param form
     */
    void mkdir(ResourceForm form);

    /**
     * 重命名
     *
     * @param form
     */
    void rename(ResourceForm form);

    /**
     * 移动
     *
     * @param form
     */
    void move(ResourceForm form);

    /**
     * 移除
     *
     * @param dbId
     * @param type
     * @param path
     */
    void remove(Integer dbId, String type, String path);

    /**
     * 获取资源目录列表
     *
     * @param dbId
     * @param path
     * @return
     */
    List<ResourceTreeVo> directoryTree(Integer dbId, String path);
}
