package com.pufferfishscheduler.service.database;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.DbField;
import com.pufferfishscheduler.domain.model.DatabaseField;

import java.util.List;

/**
 *
 * @author Mayc
 * @since 2025-09-07  01:03
 */
public interface DbFieldService extends IService<DbField> {

    /**
     * 根据表id获取字段信息
     *
     * @param tableId
     * @return
     */
    List<DatabaseField> getFieldsByTableId(Integer tableId);
}
