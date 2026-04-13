package com.pufferfishscheduler.master.database.database.service.impl;

import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.Base64Util;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.common.utils.RSAUtil;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.dao.mapper.DbDatabaseMapper;
import com.pufferfishscheduler.domain.form.database.DbDatabaseForm;
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;
import com.pufferfishscheduler.domain.vo.database.DatabaseVo;
import com.pufferfishscheduler.master.database.connect.ftp.AbstractFTPConnector;
import com.pufferfishscheduler.master.database.connect.ftp.FTPConnectorFactory;
import com.pufferfishscheduler.master.database.connect.mq.AbstractMQConnector;
import com.pufferfishscheduler.master.database.connect.mq.MQConnectorFactory;
import com.pufferfishscheduler.master.database.connect.nosql.AbstractNoSqlConnector;
import com.pufferfishscheduler.master.database.connect.nosql.NoSqlConnectorFactory;
import com.pufferfishscheduler.master.database.connect.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.master.database.connect.relationdb.DatabaseConnectorFactory;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.database.database.service.DbGroupService;
import com.pufferfishscheduler.master.common.dict.service.DictService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 数据源服务实现类
 *
 * @author mayc
 * @since 2025-06-03 21:22:29
 */
@Slf4j
@Service
public class DbDatabaseServiceImpl extends ServiceImpl<DbDatabaseMapper, DbDatabase> implements DbDatabaseService {

    /**
     * 需要校验Schema的数据库类型
     */
    private static final Set<String> SCHEMA_REQUIRED_TYPES = Set.of(
            Constants.DATABASE_TYPE.ORACLE,
            Constants.DATABASE_TYPE.DM8
    );

    /**
     * 关系型数据库类型集合
     */
    private static final Set<String> RELATIONAL_DB_TYPES = Set.of(
            Constants.DATABASE_TYPE.MYSQL,
            Constants.DATABASE_TYPE.ORACLE,
            Constants.DATABASE_TYPE.POSTGRESQL,
            Constants.DATABASE_TYPE.SQL_SERVER,
            Constants.DATABASE_TYPE.DM8,
            Constants.DATABASE_TYPE.DORIS,
            Constants.DATABASE_TYPE.STAR_ROCKS
    );

    /**
     * FTP类型集合
     */
    private static final Set<String> FTP_TYPES = Set.of(
            Constants.FTP_TYPE.FTP,
            Constants.FTP_TYPE.FTPS
    );

    /**
     * NoSQL类型集合
     */
    private static final Set<String> NOSQL_TYPES = Set.of(
            Constants.DATABASE_TYPE.REDIS,
            Constants.DATABASE_TYPE.MONGODB
    );

    /**
     * MQ类型集合
     */
    private static final Set<String> MQ_TYPES = Set.of(
            Constants.DATABASE_TYPE.KAFKA,
            Constants.DATABASE_TYPE.RABBITMQ
    );

    @Autowired
    private AESUtil aesUtil;

    @Autowired
    private RSAUtil rsaUtil;

    @Autowired
    private DbGroupService dbGroupService;

    @Autowired
    private DbDatabaseMapper dbDatabaseDao;

    @Autowired
    @Lazy
    private DictService dictService;

    // ==================== 查询方法 ====================

    @Override
    public IPage<DatabaseVo> list(Integer groupId, Integer dbId, String name, Integer pageNo, Integer pageSize) {
        Map<Integer, String> groupMap = getGroupNameMap();
        LambdaQueryWrapper<DbDatabase> queryWrapper = buildQueryWrapper(groupId, dbId, name);
        Page<DbDatabase> page = dbDatabaseDao.selectPage(new Page<>(pageNo, pageSize), queryWrapper);

        List<DatabaseVo> databaseList = page.getRecords().stream()
                .map(database -> convertToDatabaseVo(database, groupMap))
                .collect(Collectors.toList());

        return convertToPageResult(databaseList, page);
    }

    @Override
    public List<DbDatabase> listByCategory(String category) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (StringUtils.isNotBlank(category)) {
            queryWrapper.eq(DbDatabase::getCategory, category);
        }

        return dbDatabaseDao.selectList(queryWrapper);
    }

    @Override
    public List<DbDatabase> listFTPDatabaseList(String name) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(DbDatabase::getType, FTP_TYPES)
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (StringUtils.isNotBlank(name)) {
            queryWrapper.like(DbDatabase::getName, name);
        }

        List<DbDatabase> result = dbDatabaseDao.selectList(queryWrapper);
        return CollectionUtils.isEmpty(result) ? Collections.emptyList() : result;
    }

    @Override
    public List<DbDatabase> listDatabasesByGroupId(int groupId) {
        if (groupId <= 0) {
            return Collections.emptyList();
        }

        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<DbDatabase>()
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE)
                .eq(DbDatabase::getGroupId, groupId);

        List<DbDatabase> list = dbDatabaseDao.selectList(queryWrapper);
        return CollectionUtils.isEmpty(list) ? Collections.emptyList() : list;
    }

    @Override
    public List<DbDatabase> listDatabasesByGroupIds(Set<Integer> groupIds, String category) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return Collections.emptyList();
        }

        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<DbDatabase>()
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE)
                .in(DbDatabase::getGroupId, groupIds);

        if (StringUtils.isNotBlank(category)) {
            queryWrapper.eq(DbDatabase::getCategory, category);
        }

        List<DbDatabase> list = dbDatabaseDao.selectList(queryWrapper);
        return CollectionUtils.isEmpty(list) ? Collections.emptyList() : list;
    }

    @Override
    public DatabaseVo detail(Integer id) {
        validateId(id, "数据源id");

        DbDatabase dbDatabase = getDatabaseById(id);
        DbGroup group = dbGroupService.getById(dbDatabase.getGroupId());
        Map<Integer, String> groupMap = Map.of(group.getId(), group.getName());
        
        DatabaseVo databaseVo = convertToDatabaseVo(dbDatabase, groupMap);
        if (!Constants.DATABASE_TYPE.KAFKA.equals(dbDatabase.getType())) {
            databaseVo.setPassword(decryptAndEncodePassword(dbDatabase.getPassword()));
        }
        return databaseVo;
    }

    @Override
    public DbDatabase getDatabaseById(Integer id) {
        validateId(id, "数据源id");

        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getId, id)
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        DbDatabase database = dbDatabaseDao.selectOne(queryWrapper);
        if (Objects.isNull(database)) {
            throw new BusinessException(String.format("数据源[id=%s]不存在", id));
        }

        return database;
    }

    @Override
    public DBConnectionInfo getDatabaseConnectionInfo(Integer id) {
        DbDatabase info = getDatabaseById(id);
        return DBConnectionInfo.builder()
                .type(info.getType())
                .dbHost(info.getDbHost())
                .dbPort(String.valueOf(info.getDbPort()))
                .dbName(info.getDbName())
                .username(info.getUsername())
                .password(info.getPassword())
                .dbSchema(info.getDbSchema())
                .build();
    }

    @Override
    public void validateDbExist(Integer dbId) {
        validateId(dbId, "数据源id");

        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getId, dbId)
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        DbDatabase database = dbDatabaseDao.selectOne(queryWrapper);
        if (database == null) {
            throw new BusinessException(String.format("数据源[id=%s]不存在", dbId));
        }
    }

    // ==================== 增删改方法 ====================

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(DbDatabaseForm form) {
        validateForm(form);
        verifyDbNameExisted(form.getName(), form.getGroupId());

        DbDatabase dbDatabase = buildDatabaseFromForm(form);
        dbDatabase.setCreatedBy(UserContext.getCurrentAccount());
        dbDatabase.setCreatedTime(new Date());

        setConnectionInfoAndConfig(dbDatabase, form);
        dbDatabaseDao.insert(dbDatabase);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(DbDatabaseForm form) {
        validateId(form.getId(), "数据源id");

        DbDatabase dbDatabase = getDatabaseById(form.getId());
        verifyDbNameExisted(form.getName(), form.getGroupId());

        updateDatabaseFromForm(dbDatabase, form);
        dbDatabase.setUpdatedBy(UserContext.getCurrentAccount());
        dbDatabase.setUpdatedTime(new Date());

        setConnectionInfoAndConfig(dbDatabase, form);
        dbDatabaseDao.updateById(dbDatabase);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(Integer id) {
        validateId(id, "数据源id");

        UpdateWrapper<DbDatabase> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        dbDatabaseDao.update(null, updateWrapper);
    }

    // ==================== 连接测试方法 ====================

    @Override
    public void testConnect(DbDatabaseForm form) {
        validateForm(form);
        DBConnectionInfo connectionInfo = new DBConnectionInfo();
        BeanUtils.copyProperties(form, connectionInfo);
        connect(connectionInfo);
    }

    @Override
    public void connect(DBConnectionInfo connectionInfo) {
        String type = connectionInfo.getType();

        if (RELATIONAL_DB_TYPES.contains(type)) {
            connectRelationalDatabase(connectionInfo);
        } else if (FTP_TYPES.contains(type)) {
            connectFTP(connectionInfo);
        } else if (NOSQL_TYPES.contains(type)) {
            connectNoSQL(connectionInfo);
        } else if (MQ_TYPES.contains(type)) {
            connectMQ(connectionInfo);
        } else {
            throw new BusinessException(String.format("未知的数据源类型: %s", type));
        }
    }

    @Override
    public AbstractDatabaseConnector buildDbConnector(DBConnectionInfo databaseInfo) {
        AbstractDatabaseConnector connector = DatabaseConnectorFactory.getConnector(databaseInfo.getType());
        connector.setDbName(databaseInfo.getDbName());
        connector.setUsername(databaseInfo.getUsername());
        connector.setPassword(resolvePassword(databaseInfo));
        connector.setHost(databaseInfo.getDbHost().trim());

        if (StringUtils.isNotBlank(databaseInfo.getDbPort())) {
            connector.setPort(Integer.parseInt(databaseInfo.getDbPort()));
        }

        connector.setSchema(databaseInfo.getDbSchema());
        connector.setProperties(resolveProperties(databaseInfo));
        connector.setExtConfig(resolveExtConfig(databaseInfo));
        connector.setType(databaseInfo.getType());
        connector.build();

        return connector;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 获取分组名称映射
     */
    private Map<Integer, String> getGroupNameMap() {
        List<DbGroup> dbGroups = dbGroupService.getGroupList(null);
        return dbGroups.stream()
                .collect(Collectors.toMap(DbGroup::getId, DbGroup::getName));
    }

    /**
     * 构建查询条件
     */
    /**
     * 构建查询条件
     */
    private LambdaQueryWrapper<DbDatabase> buildQueryWrapper(Integer groupId, Integer dbId, String name) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (groupId != null) {
            queryWrapper.eq(DbDatabase::getGroupId, groupId);
        }

        if (dbId != null) {
            queryWrapper.eq(DbDatabase::getId, dbId);
        }

        if (StringUtils.isNotBlank(name)) {
            queryWrapper.like(DbDatabase::getName, name);
        }

        return queryWrapper;
    }

    /**
     * 转换为分页结果
     */
    private Page<DatabaseVo> convertToPageResult(List<DatabaseVo> records, Page<DbDatabase> sourcePage) {
        Page<DatabaseVo> result = new Page<>();
        result.setRecords(records);
        result.setTotal(sourcePage.getTotal());
        result.setSize(sourcePage.getSize());
        result.setCurrent(sourcePage.getCurrent());
        return result;
    }

    /**
     * 转换为DatabaseVo
     */
    private DatabaseVo convertToDatabaseVo(DbDatabase dbDatabase, Map<Integer, String> groupMap) {
        DatabaseVo vo = new DatabaseVo();
        BeanUtils.copyProperties(dbDatabase, vo);
        vo.setGroupName(groupMap.getOrDefault(vo.getGroupId(), ""));
        vo.setLabelName(dictService.getDictItemValue(Constants.DICT.DATA_SOURCE_LAYERING, dbDatabase.getLabel()));
        vo.setCategoryName(dictService.getDictItemValue(Constants.DICT.DATABASE_CATEGORY, dbDatabase.getCategory()));
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));

        if (isInvalidProperties(dbDatabase.getProperties())) {
            vo.setProperties(null);
        }
        return vo;
    }

    /**
     * 判断properties是否无效
     */
    private boolean isInvalidProperties(String properties) {
        return StringUtils.isNotBlank(properties) && "{}".equals(properties);
    }

    /**
     * 从Form构建Database实体
     */
    private DbDatabase buildDatabaseFromForm(DbDatabaseForm form) {
        DbDatabase dbDatabase = new DbDatabase();
        BeanUtils.copyProperties(form, dbDatabase, "password");

        if (isRelationalDatabase(form.getType())) {
            validateRelationalDbCredentials(form.getUsername(), form.getPassword());
            if (StringUtils.isNotBlank(form.getPassword())) {
                dbDatabase.setPassword(encryptPassword(form.getPassword()));
            }
        }

        return dbDatabase;
    }

    /**
     * 从Form更新Database实体
     */
    private void updateDatabaseFromForm(DbDatabase dbDatabase, DbDatabaseForm form) {
        String encryptedPassword = encryptPassword(form.getPassword());
        BeanUtils.copyProperties(form, dbDatabase, "password");
        dbDatabase.setPassword(encryptedPassword);
    }

    /**
     * 设置连接信息和配置
     */
    private void setConnectionInfoAndConfig(DbDatabase dbDatabase, DbDatabaseForm form) {
        DBConnectionInfo connectionInfo = new DBConnectionInfo();
        BeanUtils.copyProperties(form, connectionInfo);

        dbDatabase.setProperties(buildPropertiesJson(connectionInfo));
        dbDatabase.setExtConfig(buildExtConfigJson(connectionInfo));
    }

    /**
     * 构建Properties JSON
     */
    private String buildPropertiesJson(DBConnectionInfo connectionInfo) {
        JSONObject properties = new JSONObject();

        Optional.ofNullable(connectionInfo.getMode())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> properties.put(Constants.FTP_PROPERTIES.MODE, v));

        Optional.ofNullable(connectionInfo.getControlEncoding())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> properties.put(Constants.FTP_PROPERTIES.CONTROL_ENCODING, v));

        return properties.toJSONString();
    }

    /**
     * 构建扩展配置JSON
     */
    private String buildExtConfigJson(DBConnectionInfo connectionInfo) {
        JSONObject extConfig = new JSONObject();

        Optional.ofNullable(connectionInfo.getFeAddress())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> extConfig.put(Constants.DATABASE_EXT_CONFIG.FE_ADDRESS, v));

        Optional.ofNullable(connectionInfo.getBeAddress())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> extConfig.put(Constants.DATABASE_EXT_CONFIG.BE_ADDRESS, v));

        return extConfig.toJSONString();
    }

    /**
     * 密码加密
     */
    private String encryptPassword(String password) {
        if (StringUtils.isBlank(password)) {
            return null;
        }
        String decrypted = rsaUtil.decrypt(password);
        return aesUtil.encrypt(decrypted);
    }

    /**
     * 密码解密并Base64编码
     */
    private String decryptAndEncodePassword(String encryptedPassword) {
        try {
            String decrypted = aesUtil.decrypt(encryptedPassword);
            return Base64Util.encode(decrypted);
        } catch (Exception e) {
            log.error("密码解密失败", e);
            throw new BusinessException("数据源密码解密失败!");
        }
    }

    /**
     * 解析密码（优先使用传入密码，否则从数据库获取）
     */
    private String resolvePassword(DBConnectionInfo databaseInfo) {
        if (StringUtils.isNotBlank(databaseInfo.getPassword())) {
            return rsaUtil.decrypt(databaseInfo.getPassword());
        }

        if (databaseInfo.getId() != null) {
            DbDatabase dbDatabase = dbDatabaseDao.selectById(databaseInfo.getId());
            if (dbDatabase != null) {
                return aesUtil.decrypt(dbDatabase.getPassword());
            }
        }

        return null;
    }

    /**
     * 解析Properties
     */
    private String resolveProperties(DBConnectionInfo databaseInfo) {
        if (StringUtils.isNotBlank(databaseInfo.getProperties())) {
            return databaseInfo.getProperties();
        }

        JSONObject param = new JSONObject();
        Optional.ofNullable(databaseInfo.getMode())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> param.put(Constants.FTP_PROPERTIES.MODE, v));

        Optional.ofNullable(databaseInfo.getControlEncoding())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> param.put(Constants.FTP_PROPERTIES.CONTROL_ENCODING, v));

        return param.toJSONString();
    }

    /**
     * 解析扩展配置
     */
    private String resolveExtConfig(DBConnectionInfo databaseInfo) {
        JSONObject extConfig = new JSONObject();

        Optional.ofNullable(databaseInfo.getFeAddress())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> extConfig.put(Constants.DATABASE_EXT_CONFIG.FE_ADDRESS, v));

        Optional.ofNullable(databaseInfo.getBeAddress())
                .filter(StringUtils::isNotBlank)
                .ifPresent(v -> extConfig.put(Constants.DATABASE_EXT_CONFIG.BE_ADDRESS, v));

        return extConfig.toJSONString();
    }

    /**
     * 连接关系型数据库
     */
    private void connectRelationalDatabase(DBConnectionInfo connectionInfo) {
        validateRelationalDbCredentials(connectionInfo.getUsername(), connectionInfo.getPassword());

        // Oracle和达梦需要校验Schema
        if (SCHEMA_REQUIRED_TYPES.contains(connectionInfo.getType()) && StringUtils.isBlank(connectionInfo.getDbSchema())) {
            throw new BusinessException("请校验模式(Schema)是否为空!");
        }

        AbstractDatabaseConnector connector = buildDbConnector(connectionInfo);
        ConResponse response = connector.connect(connectionInfo.getType());

        if (!response.getResult()) {
            throw new BusinessException(response.getMsg());
        }
    }

    /**
     * 连接NoSQL数据库
     */
    private void connectNoSQL(DBConnectionInfo connectionInfo) {
        AbstractNoSqlConnector connector = NoSqlConnectorFactory.getConnector(connectionInfo.getType());
        connector.setId(connectionInfo.getId());
        connector.setDbHost(connectionInfo.getDbHost());
        connector.setDbPort(connectionInfo.getDbPort());
        connector.setType(connectionInfo.getType());
        connector.setUsername(connectionInfo.getUsername());
        connector.setPassword(resolvePassword(connectionInfo));
        connector.setDatabaseIndex(connectionInfo.getDatabaseIndex());
        connector.setProperties(connectionInfo.getProperties());

        ConResponse response = connector.connect();
        if (!response.getResult()) {
            throw new BusinessException(response.getMsg());
        }
    }

    /**
     * 连接消息队列
     */
    private void connectMQ(DBConnectionInfo connectionInfo) {
        AbstractMQConnector connector = MQConnectorFactory.getConnector(connectionInfo.getType());
        connector.setId(connectionInfo.getId());
        connector.setDbHost(connectionInfo.getDbHost());
        connector.setDbPort(connectionInfo.getDbPort());
        if (Constants.DATABASE_TYPE.RABBITMQ.equals(connectionInfo.getType())) {
            connector.setUsername(connectionInfo.getUsername());
            connector.setPassword(resolvePassword(connectionInfo));
        }
        connector.setType(connectionInfo.getType());
        connector.setClientId(connectionInfo.getClientId());
        connector.setTopic(connectionInfo.getTopic());
        connector.setQueue(connectionInfo.getQueue());
        connector.setProperties(connectionInfo.getProperties());

        ConResponse response = connector.connect();
        if (!response.getResult()) {
            throw new BusinessException(response.getMsg());
        }
    }

    /**
     * 连接FTP/FTPS
     */
    private void connectFTP(DBConnectionInfo connectionInfo) {
        AbstractFTPConnector connector = FTPConnectorFactory.getConnector(connectionInfo.getType());
        connector.setHost(connectionInfo.getDbHost());
        connector.setPort(Integer.parseInt(connectionInfo.getDbPort()));
        connector.setUsername(connectionInfo.getUsername());
        connector.setPassword(connectionInfo.getPassword());
        connector.setMode(connectionInfo.getMode());
        connector.setControlEncoding(connectionInfo.getControlEncoding());
        connector.connect();
    }

    /**
     * 验证表单
     */
    private void validateForm(DbDatabaseForm form) {
        if (form == null) {
            throw new BusinessException("数据源信息不能为空");
        }
        if (StringUtils.isBlank(form.getName())) {
            throw new BusinessException("数据源名称不能为空");
        }
        if (form.getGroupId() == null) {
            throw new BusinessException("数据源分组不能为空");
        }
    }

    /**
     * 验证ID
     */
    private void validateId(Integer id, String fieldName) {
        if (Objects.isNull(id)) {
            throw new BusinessException(String.format("%s不能为空", fieldName));
        }
    }

    /**
     * 验证关系型数据库凭证
     */
    private void validateRelationalDbCredentials(String username, String password) {
        if (StringUtils.isBlank(username)) {
            throw new BusinessException("数据库用户名不能为空");
        }
        if (StringUtils.isBlank(password)) {
            throw new BusinessException("数据库密码不能为空");
        }
    }

    /**
     * 判断是否为关系型数据库
     */
    private boolean isRelationalDatabase(String type) {
        return RELATIONAL_DB_TYPES.contains(type);
    }

    /**
     * 验证相同分组下数据源名称是否存在
     */
    private void verifyDbNameExisted(String name, Integer groupId) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getDbName, name)
                .eq(DbDatabase::getGroupId, groupId)
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        DbDatabase database = dbDatabaseDao.selectOne(queryWrapper);
        if (Objects.nonNull(database)) {
            throw new BusinessException("当前分组下数据源名称已存在");
        }
    }
}