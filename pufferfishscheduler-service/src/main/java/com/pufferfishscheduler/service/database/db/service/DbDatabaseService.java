package com.pufferfishscheduler.service.database.db.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;
import com.pufferfishscheduler.domain.form.database.DbDatabaseForm;
import com.pufferfishscheduler.domain.vo.database.DatabaseVo;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.domain.model.DatabaseConnectionInfo;
import com.pufferfishscheduler.service.database.db.connector.relationdb.AbstractDatabaseConnector;

import java.util.List;
import java.util.Set;

/**
 * (DbDatabase) Service
 *
 * @author mayc
 * @since 2025-06-03 21:22:28
 */
public interface DbDatabaseService extends IService<DbDatabase> {

    /**
     * 获取数据源集合
     *
     * @param groupId
     * @param dbId
     * @param name
     * @param pageNo
     * @param pageSize
     * @return
     */
    IPage<DatabaseVo> list(Integer groupId, Integer dbId, String name, Integer pageNo, Integer pageSize);

    /**
     * 获取FTP数据源集合
     *
     * @param name
     * @return
     */
    List<DbDatabase> listFTPDatabaseList(String name);

    /**
     * 根据分组id获取数据源集合
     *
     * @param groupId
     * @return
     */
    List<DbDatabase> listDatabasesByGroupId(int groupId);

    /**
     * 通过分组id集合获取数据源集合
     *
     * @param groupIds
     * @return
     */
    List<DbDatabase> listDatabasesByGroupIds(Set<Integer> groupIds);


    /**
     * 接入数据源
     *
     * @param form
     */
    void add(DbDatabaseForm form);

    /**
     * 修改数据源
     *
     * @param form
     */
    void update(DbDatabaseForm form);

    /**
     * 删除数据源
     *
     * @param id
     */
    void delete(Integer id);

    /**
     * 获取数据源详情
     *
     * @param id
     * @return
     */
    DatabaseVo detail(Integer id);

    /**
     * 获取数据源基本信息
     *
     * @param id
     * @return
     */
    DbDatabase getDatabaseById(int id);

    /**
     * 构建数据源连接器
     *
     * @param databaseInfo
     * @return
     */
    AbstractDatabaseConnector buildDbConnector(DatabaseConnectionInfo databaseInfo);

    /**
     * 测试连接
     *
     * @param form
     */
    void testConnect(DbDatabaseForm form);

    /**
     * 连接数据源
     *
     * @param databaseInfo
     */
    void connect(DatabaseConnectionInfo databaseInfo);

    /**
     * 验证数据源是否存在
     *
     * @param dbId
     */
    void validateDbExist(Integer dbId);
}

