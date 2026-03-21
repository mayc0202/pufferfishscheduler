package com.pufferfishscheduler.master.collect.trans.plugin.service.impl;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.domain.form.collect.FieldStreamForm;
import com.pufferfishscheduler.domain.model.database.DatabaseField;
import com.pufferfishscheduler.domain.vo.collect.FieldMappingVo;
import com.pufferfishscheduler.master.collect.trans.plugin.service.TableOutputService;
import com.pufferfishscheduler.master.collect.trans.service.TransFlowService;
import com.pufferfishscheduler.master.database.database.service.DbFieldService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 表输出插件服务实现类
 * 
 * @author mayc
 */
@Service
public class TableOutputServiceImpl implements TableOutputService {

    @Autowired
    private TransFlowService transFlowService;

    @Autowired
    private DbFieldService dbFieldService;

    @Override
    public List<FieldMappingVo> getFieldStream(FieldStreamForm form) {
        // 1. 参数校验
        validateParams(form);

        // 2. 获取流字段和表字段
        String[] streamFields = getStreamFields(form);
        if (streamFields == null || streamFields.length == 0) {
            return new ArrayList<>();
        }
        
        List<DatabaseField> tableFields = getTableFields(form.getTableId());

        // 3. 构建字段映射
        return buildFieldMappings(streamFields, tableFields);
    }

    /**
     * 校验参数
     * 
     * @param form
     */
    private void validateParams(FieldStreamForm form) {
        if (form.getDbId() == null) {
            throw new BusinessException("数据源id不能为空");
        }
        if (form.getTableId() == null) {
            throw new BusinessException("表id不能为空");
        }
    }

    /**
     * 获取流字段
     * 
     * @param form
     * @return
     */
    private String[] getStreamFields(FieldStreamForm form) {
        return transFlowService.getFieldStream(
            form.getFlowId(), 
            form.getConfig(),
            form.getStepName(), 
            null
        );
    }

    /**
     * 获取表字段
     * 
     * @param tableId
     * @return
     */
    private List<DatabaseField> getTableFields(Integer tableId) {
        return dbFieldService.getFieldsByTableId(tableId);
    }

    /**
     * 构建字段映射
     * 
     * @param streamFields
     * @param tableFields
     * @return
     */
    private List<FieldMappingVo> buildFieldMappings(String[] streamFields, List<DatabaseField> tableFields) {
        // 构建表字段映射
        Map<String, DatabaseField> tableFieldMap = buildTableFieldMap(tableFields);
        Set<String> matchedFields = new HashSet<>();
        List<FieldMappingVo> mappings = new ArrayList<>();

        // 处理流字段
        for (String streamField : streamFields) {
            FieldMappingVo vo = new FieldMappingVo();
            vo.setFieldStream(streamField);
            
            DatabaseField matchedField = findMatchedField(streamField, tableFieldMap);
            if (matchedField != null) {
                vo.setFieldDatabase(matchedField.getName());
                matchedFields.add(matchedField.getName().toLowerCase());
            } else {
                vo.setFieldDatabase("");
            }
            
            mappings.add(vo);
        }

        // 处理未匹配的表字段
        for (DatabaseField tableField : tableFields) {
            if (!matchedFields.contains(tableField.getName().toLowerCase())) {
                FieldMappingVo vo = new FieldMappingVo();
                vo.setFieldDatabase(tableField.getName());
                vo.setFieldStream("");
                mappings.add(vo);
            }
        }

        return mappings;
    }

    /**
     * 构建表字段映射
     * 
     * @param tableFields
     * @return
     */
    private Map<String, DatabaseField> buildTableFieldMap(List<DatabaseField> tableFields) {
        Map<String, DatabaseField> fieldMap = new HashMap<>();
        
        for (DatabaseField field : tableFields) {
            fieldMap.put(field.getName().toLowerCase(), field);
            if (StringUtils.isNotBlank(field.getBusinessName())) {
                fieldMap.put(field.getBusinessName().toLowerCase(), field);
            }
        }
        
        return fieldMap;
    }

    /**
     * 查找匹配的表字段
     * 
     * @param streamField
     * @param tableFieldMap
     * @return
     */
    private DatabaseField findMatchedField(String streamField, Map<String, DatabaseField> tableFieldMap) {
        if (StringUtils.isBlank(streamField)) {
            return null;
        }
        return tableFieldMap.get(streamField.toLowerCase());
    }

}
