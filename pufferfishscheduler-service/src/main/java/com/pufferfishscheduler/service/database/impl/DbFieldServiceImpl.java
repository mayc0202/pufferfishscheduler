package com.pufferfishscheduler.service.database.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.domain.model.DatabaseField;
import com.pufferfishscheduler.dao.entity.DbField;
import com.pufferfishscheduler.dao.mapper.DbFieldMapper;
import com.pufferfishscheduler.service.database.DbFieldService;
import com.pufferfishscheduler.service.database.DbTableService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
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
        return databaseField;
    }
}
