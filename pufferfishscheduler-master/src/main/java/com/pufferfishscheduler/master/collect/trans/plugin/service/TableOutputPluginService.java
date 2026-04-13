package com.pufferfishscheduler.master.collect.trans.plugin.service;

import java.util.List;

import com.pufferfishscheduler.domain.form.collect.FieldStreamForm;
import com.pufferfishscheduler.domain.vo.collect.FieldMappingVo;

/**
 * 表输出插件服务接口
 * 
 * @author mayc
 */
public interface TableOutputPluginService {

    /**
     * 获取转换流字段流
     *
     * @param form 字段流表单
     * @return 字段流
     */
    List<FieldMappingVo> getFieldStream(FieldStreamForm form);

}
