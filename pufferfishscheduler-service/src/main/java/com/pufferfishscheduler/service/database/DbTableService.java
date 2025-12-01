package com.pufferfishscheduler.service.database;

import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.dao.entity.DbTable;
import com.pufferfishscheduler.domain.model.DatabaseTable;

import java.util.List;

/**
 *
 * @author Mayc
 * @since 2025-09-07  01:02
 */
public interface DbTableService extends IService<DbTable> {

    /**
     * 根据数据源Id获取数据库下表集合
     *
     * @param dbId
     * @return
     */
    List<DatabaseTable> getTablesByDbId(Integer dbId);


    /**
     * 验证表是否存在
     *
     * @param tableId
     */
    void validateTableExist(Integer tableId);

}
