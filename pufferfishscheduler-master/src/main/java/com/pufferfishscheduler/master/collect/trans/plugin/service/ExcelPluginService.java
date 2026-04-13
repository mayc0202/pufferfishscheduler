package com.pufferfishscheduler.master.collect.trans.plugin.service;

import com.pufferfishscheduler.domain.form.collect.TransConfigForm;
import com.pufferfishscheduler.domain.vo.collect.BaseFieldVo;
import com.pufferfishscheduler.domain.vo.database.ResourceVo;

import java.util.List;
import java.util.Set;

/**
 * Excel插件服务
 */
public interface ExcelPluginService {

    /**
     * 获取所有的文件
     *
     * @param dbId 数据库ID
     * @param path 路径
     * @return {@link List}<{@link ResourceVo}>
     */
    List<ResourceVo> getResources(Integer dbId, String path);

    /**
     * 获取所有的sheet名称
     *
     * @param form 配置表单
     * @return {@link Set}<{@link String}>
     */
    Set<String> getSheets(TransConfigForm form);

    /**
     * 获取所有的字段
     *
     * @param form 配置表单
     * @return {@link List}<{@link BaseFieldVo}>
     */
    List<BaseFieldVo> getFields(TransConfigForm form);
}
