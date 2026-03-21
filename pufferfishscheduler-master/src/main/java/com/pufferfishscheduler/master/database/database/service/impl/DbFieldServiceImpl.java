package com.pufferfishscheduler.master.database.database.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.DbField;
import com.pufferfishscheduler.dao.mapper.DbFieldMapper;
import com.pufferfishscheduler.domain.model.database.DatabaseField;
import com.pufferfishscheduler.master.database.database.service.DbFieldService;
import com.pufferfishscheduler.master.database.database.service.DbTablePropertiesService;
import com.pufferfishscheduler.master.database.database.service.DbTableService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author Mayc
 * @since 2025-09-07  01:04
 */
@Service
public class DbFieldServiceImpl extends ServiceImpl<DbFieldMapper, DbField> implements DbFieldService {

    @Autowired
    private DbFieldMapper dbFieldDao;

    @Autowired
    private DbTableService dbTableService;

    @Autowired
    private DbTablePropertiesService dbTablePropertiesService;

    /**
     * 根据表id获取字段列表
     *
     * @param tableId
     * @return
     */
    @Override
    public List<DatabaseField> getFieldsByTableId(Integer tableId) {
        // 验证表是否存在
        dbTableService.validateTableExist(tableId);

        LambdaQueryWrapper<DbField> ldq =new LambdaQueryWrapper<>();
        ldq.eq(DbField::getTableId,tableId).eq(DbField::getDeleted, Constants.DELETE_FLAG.FALSE);
        List<DbField> dbFields = dbFieldDao.selectList(ldq);
        if (dbFields.isEmpty()) {
            return Collections.emptyList();
        }

        return dbFields.stream()
                .map(this::buildDbFieldInfo)
                .collect(Collectors.toList());
    }

    /**
     * 构建DbFieldInfo
     * @param dbField
     * @return
     */
    private DatabaseField buildDbFieldInfo(DbField dbField) {
        DatabaseField databaseField = new DatabaseField();
        BeanUtils.copyProperties(dbField, databaseField);
        //判断是否为主键
        Map<String, String> primaryKeysMap = getPrimaryKeysMap(dbField.getTableId());
        if (primaryKeysMap.containsKey(dbField.getName())) {
            databaseField.setPrimaryKey(true);
        } else {
            databaseField.setPrimaryKey(false);
        }
        return databaseField;
    }

    /**
     * 获取表的主键映射
     * @param tableId
     * @return
     */
    private Map<String, String> getPrimaryKeysMap(Integer tableId) {
        Map<String, String> result = new HashMap<>();
        //获取逻辑主键
        String primaryKeySting = dbTablePropertiesService.selectTableProperties(tableId, "2");
        if (StringUtils.isEmpty(primaryKeySting)) {
            primaryKeySting = dbTablePropertiesService.selectTableProperties(tableId, "1");
        }

        if (StringUtils.isEmpty(primaryKeySting)) {
            return result;
        }

        String[] split = primaryKeySting.split(",");
        for (String sTemp : split) {
            result.put(sTemp, sTemp);
        }


        return result;
    }
}
