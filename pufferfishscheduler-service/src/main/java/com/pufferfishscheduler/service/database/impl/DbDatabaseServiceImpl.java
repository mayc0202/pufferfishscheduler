package com.pufferfishscheduler.service.database.impl;

import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.service.dict.service.DictService;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.AESUtil;
import com.pufferfishscheduler.common.utils.Base64Util;
import com.pufferfishscheduler.common.utils.DateUtil;
import com.pufferfishscheduler.common.utils.RSAUtil;
import com.pufferfishscheduler.domain.form.database.DbDatabaseForm;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.domain.vo.database.DatabaseVo;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.DbGroup;
import com.pufferfishscheduler.dao.mapper.DbDatabaseMapper;
import com.pufferfishscheduler.domain.model.DatabaseConnectionInfo;
import com.pufferfishscheduler.service.database.connect.ftp.AbstractFTPConnector;
import com.pufferfishscheduler.service.database.connect.ftp.FTPConnectorFactory;
import com.pufferfishscheduler.service.database.connect.relationdb.AbstractDatabaseConnector;
import com.pufferfishscheduler.service.database.connect.relationdb.DatabaseConnectorFactory;
import com.pufferfishscheduler.service.database.DbDatabaseService;
import com.pufferfishscheduler.service.database.DbGroupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * (DbDatabase) ServiceImpl
 *
 * @author mayc
 * @since 2025-06-03 21:22:29
 */
@Slf4j
@Service
public class DbDatabaseServiceImpl extends ServiceImpl<DbDatabaseMapper, DbDatabase> implements DbDatabaseService {

    @Autowired
    private AESUtil aesUtil;

    @Autowired
    private RSAUtil rsaUtil;

    @Autowired
    private DbGroupService dbGroupService;

    @Autowired
    private DbDatabaseMapper dbDatabaseDao;

    @Autowired
    private DictService dictService;

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
    @Override
    public IPage<DatabaseVo> list(Integer groupId, Integer dbId, String name, Integer pageNo, Integer pageSize) {

        // 查询出所有分组
        List<DbGroup> dbGroups = dbGroupService.getGroupList(null);
        Map<Integer, String> groupMap = dbGroups.stream().collect(Collectors.toMap(DbGroup::getId, DbGroup::getName));

        LambdaQueryWrapper<DbDatabase> ldq = buildQueryWrapper(groupId, dbId, name);
        Page<DbDatabase> page = dbDatabaseDao.selectPage(new Page<>(pageNo, pageSize), ldq);

        List<DatabaseVo> databaseList = new ArrayList<>();
        for (DbDatabase database : page.getRecords()) {
            DatabaseVo vo = convertDatabaseVo(database, groupMap);
            databaseList.add(vo);
        }

        Page<DatabaseVo> result = new Page<>();
        result.setRecords(databaseList);
        result.setTotal(page.getTotal());
        result.setSize(page.getSize());
        result.setCurrent(page.getCurrent());

        return result;
    }

    /**
     * 构建查询条件
     *
     * @param groupId
     * @param dbId
     * @param name
     * @return
     */
    private LambdaQueryWrapper<DbDatabase> buildQueryWrapper(Integer groupId, Integer dbId, String name) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (Objects.nonNull(groupId)) {
            queryWrapper.eq(DbDatabase::getGroupId, groupId);
        }

        if (Objects.nonNull(dbId)) {
            queryWrapper.eq(DbDatabase::getId, dbId);
        }

        if (StringUtils.isNotBlank(name)) {
            queryWrapper.like(DbDatabase::getName, name);
        }

        return queryWrapper;
    }

    /**
     * 转换数据源信息为Vo
     *
     * @param dbDatabase
     * @param groupMap
     * @return
     */
    private DatabaseVo convertDatabaseVo(DbDatabase dbDatabase, Map<Integer, String> groupMap) {
        DatabaseVo vo = new DatabaseVo();
        BeanUtils.copyProperties(dbDatabase, vo);
        vo.setGroupName(groupMap.getOrDefault(vo.getGroupId(), ""));
        vo.setLabelName(dictService.getDictItemCode(Constants.DICT.DATA_SOURCE_LAYERING, dbDatabase.getLabel()));
        vo.setCategoryName(dictService.getDictItemCode(Constants.DICT.DATABASE_CATEGORY, dbDatabase.getLabel()));
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(vo.getCreatedTime()));

        if (StringUtils.isNotBlank(dbDatabase.getProperties()) && "{}".equals(dbDatabase.getProperties())) {
            vo.setProperties(null);
        }
        return vo;
    }

    /**
     * 获取FTP数据源集合
     *
     * @param name
     * @return
     */
    @Override
    public List<DbDatabase> listFTPDatabaseList(String name) {
        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.in(DbDatabase::getType, Arrays.asList(Constants.FTP_TYPE.FTP, Constants.FTP_TYPE.FTPS))
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (StringUtils.isNotBlank(name)) {
            queryWrapper.like(DbDatabase::getName, name);
        }

        List<DbDatabase> result = dbDatabaseDao.selectList(queryWrapper);
        if (result.isEmpty()) {
            return Collections.emptyList();
        }
        return result;
    }

    /**
     * 通过分组id获取数据源集合
     *
     * @param groupId
     * @return
     */
    @Override
    public List<DbDatabase> listDatabasesByGroupId(int groupId) {
        if (Objects.isNull(groupId)) {
            return Collections.emptyList();
        }

        LambdaQueryWrapper<DbDatabase> queryWrapper = new LambdaQueryWrapper<DbDatabase>()
                .eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE)
                .eq(DbDatabase::getGroupId, groupId);

        List<DbDatabase> list = dbDatabaseDao.selectList(queryWrapper);
        if (CollectionUtils.isEmpty(list)) {
            return Collections.emptyList();
        }
        return list;
    }

    /**
     * 通过分组id集合获取数据源集合
     *
     * @param groupIds 数据源分组id集合
     * @param category 数据库类型
     * @return
     */
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
        if (CollectionUtils.isEmpty(list)) {
            return Collections.emptyList();
        }
        return list;
    }


    /**
     * 添加数据源
     *
     * @param form
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(DbDatabaseForm form) {

        // 校验当前分类下数据源是否存在
        verifyDbNameExisted(form.getName(), form.getGroupId());

        // 密码入库加密
        String decrypt = aesUtil.decrypt(form.getPassword());
        String pwd = aesUtil.encrypt(decrypt);

        DbDatabase dbDatabase = new DbDatabase();
        BeanUtils.copyProperties(form, dbDatabase, "password");
        dbDatabase.setPassword(pwd);
        dbDatabase.setCreatedBy(UserContext.getCurrentAccount());
        dbDatabase.setCreatedTime(new Date());

        // 处理数据源参数
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(form, databaseConnectionInfo);

        JSONObject properties = new JSONObject();
        handleProperties(databaseConnectionInfo, properties);
        dbDatabase.setProperties(properties.toJSONString());

        // 处理扩展配置
        JSONObject extConfig = new JSONObject();
        handExtConfig(databaseConnectionInfo, extConfig);
        dbDatabase.setExtConfig(extConfig.toJSONString());

        dbDatabaseDao.insert(dbDatabase);
    }

    /**
     * 修改数据源
     *
     * @param form
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(DbDatabaseForm form) {

        if (Objects.isNull(form.getId())) {
            throw new BusinessException("请校验数据源id是否为空!");
        }

        DbDatabase dbDatabase = getDatabaseById(form.getId());
        if (Objects.isNull(dbDatabase)) {
            throw new BusinessException("请校验数据源是否存在!");
        }

        // 校验数据源是否已存在
        verifyDbNameExisted(form.getName(), form.getGroupId());

        // 密码入库加密
        String decrypt = rsaUtil.decrypt(form.getPassword());
        String pwd = aesUtil.encrypt(decrypt);

        BeanUtils.copyProperties(form, dbDatabase, "password");
        dbDatabase.setPassword(pwd);
        dbDatabase.setUpdatedBy(UserContext.getCurrentAccount());
        dbDatabase.setUpdatedTime(new Date());

        // 处理数据源参数
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(form, databaseConnectionInfo);

        JSONObject properties = new JSONObject();
        handleProperties(databaseConnectionInfo, properties);
        dbDatabase.setProperties(properties.toJSONString());

        // 处理扩展配置
        JSONObject extConfig = new JSONObject();
        handExtConfig(databaseConnectionInfo, extConfig);
        dbDatabase.setExtConfig(extConfig.toJSONString());

        dbDatabaseDao.updateById(dbDatabase);
    }

    /**
     * 删除数据源
     *
     * @param id
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(Integer id) {

        if (Objects.isNull(id)) {
            throw new BusinessException("请校验数据源id是否为空!");
        }

        // 使用 UpdateWrapper 进行更新
        UpdateWrapper<DbDatabase> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", id)
                .eq("deleted", Constants.DELETE_FLAG.FALSE)
                .set("deleted", Constants.DELETE_FLAG.TRUE)
                .set("updated_by", UserContext.getCurrentAccount())
                .set("updated_time", new Date());

        dbDatabaseDao.update(null, updateWrapper);
    }

    /**
     * 获取数据源详情
     *
     * @param id
     * @return
     */
    @Override
    public DatabaseVo detail(Integer id) {
        if (Objects.isNull(id)) {
            throw new BusinessException("请校验数据源id是否为空!");
        }

        DbDatabase dbDatabase = getDatabaseById(id);
        DbGroup group = dbGroupService.getById(dbDatabase.getGroupId());
        Map<Integer, String> groupMap = new HashMap<>();
        groupMap.put(group.getId(), group.getName());

        DatabaseVo databaseVo = convertDatabaseVo(dbDatabase, groupMap);

        // AES解密再Base64加密
        try {
            String decrypt = aesUtil.decrypt(dbDatabase.getPassword());
            String encode = Base64Util.encode(decrypt);
            databaseVo.setPassword(encode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(String.format("数据源[%s]密码解密失败!", databaseVo.getDbName()));
        }

        return databaseVo;
    }

    /**
     * 测试连接
     *
     * @param form
     */
    @Override
    public void testConnect(DbDatabaseForm form) {
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(form, databaseConnectionInfo);
        connect(databaseConnectionInfo);
    }

    /**
     * 数据源连接
     *
     * @param databaseConnectionInfo
     */
    @Override
    public void connect(DatabaseConnectionInfo databaseConnectionInfo) {
        switch (databaseConnectionInfo.getType()) {
            case Constants.DATABASE_TYPE.MYSQL:
            case Constants.DATABASE_TYPE.ORACLE:
            case Constants.DATABASE_TYPE.POSTGRESQL:
            case Constants.DATABASE_TYPE.SQL_SERVER:
            case Constants.DATABASE_TYPE.DM8:
            case Constants.DATABASE_TYPE.DORIS:
            case Constants.DATABASE_TYPE.STAR_ROCKS:
                connectDatabase(databaseConnectionInfo);
                break;
            case Constants.FTP_TYPE.FTP:
            case Constants.FTP_TYPE.FTPS:
                connectFTP(databaseConnectionInfo);
                break;
            case Constants.DATABASE_TYPE.REDIS:
                break;
            case Constants.DATABASE_TYPE.MONGODB:
                break;
            case Constants.DATABASE_TYPE.KAFKA:
                break;
            default:
                throw new BusinessException(String.format("未知的数据源类型:%s!", databaseConnectionInfo.getType()));
        }
    }

    /**
     * 验证数据源是否存在
     *
     * @param dbId
     */
    @Override
    public void validateDbExist(Integer dbId) {
        LambdaQueryWrapper<DbDatabase> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbDatabase::getId, dbId).eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        DbDatabase database = dbDatabaseDao.selectOne(ldq);
        if (database == null) {
            throw new BusinessException(String.format("请校验id[%s]数据源是否已存在!", dbId));
        }
    }

    /**
     * 获取数据源信息
     *
     * @param id
     * @return
     */
    @Override
    public DbDatabase getDatabaseById(int id) {
        LambdaQueryWrapper<DbDatabase> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbDatabase::getId, id).eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);

        DbDatabase database = dbDatabaseDao.selectOne(ldq);
        if (Objects.isNull(database)) {
            throw new BusinessException(String.format("请校验id[%s]数据源是否已存在!", id));
        }

        return database;
    }

    /**
     * 构建数据源连接器
     *
     * @param databaseInfo
     * @return
     */
    @Override
    public AbstractDatabaseConnector buildDbConnector(DatabaseConnectionInfo databaseInfo) {
        // 获取连接器
        AbstractDatabaseConnector connector = DatabaseConnectorFactory.getConnector(databaseInfo.getType());
        connector.setDbName(databaseInfo.getDbName());
        connector.setUsername(databaseInfo.getUsername());

        //判断密码是否为空
        if (StringUtils.isNotBlank(databaseInfo.getPassword())) {
            // 解密
            String decrypt = rsaUtil.decrypt(databaseInfo.getPassword());
            connector.setPassword(decrypt);
        } else {
            DbDatabase dbDatabase = dbDatabaseDao.selectById(databaseInfo.getId());
            String decodePassword = aesUtil.decrypt(dbDatabase.getPassword());
            connector.setPassword(decodePassword);
        }

        connector.setHost(databaseInfo.getDbHost().trim());
        if (StringUtils.isNotBlank(databaseInfo.getDbPort())) {
            connector.setPort(Integer.parseInt(databaseInfo.getDbPort()));
        }
        connector.setSchema(databaseInfo.getDbSchema());

        // 处理参数
        JSONObject param = new JSONObject();
        handleProperties(databaseInfo, param);

        //获取数据库中已保存的properties
        if (StringUtils.isNotBlank(databaseInfo.getProperties())) {
            connector.setProperties(databaseInfo.getProperties());
        } else {
            connector.setProperties(param.toJSONString());
        }

        JSONObject extConfig = new JSONObject();
        handExtConfig(databaseInfo, extConfig);
        connector.setExtConfig(extConfig.toJSONString());

        connector.setType(databaseInfo.getType());

        connector.build();

        return connector;
    }

    /**
     * 连接数据库
     *
     * @param databaseInfo
     */
    public void connectDatabase(DatabaseConnectionInfo databaseInfo) {

        // 单独校验schema
        if ((Constants.DATABASE_TYPE.ORACLE.equals(databaseInfo.getType()) || Constants.DATABASE_TYPE.DM8.equals(databaseInfo.getType())) && StringUtils.isBlank(databaseInfo.getDbSchema())) {
            throw new BusinessException("请校验模式(Schema)是否为空!");
        }

        // 获取连接器
        AbstractDatabaseConnector connector = buildDbConnector(databaseInfo);
        ConResponse response = connector.connect(databaseInfo.getType());

        // 判断ConResponse的标识，如果为false，则抛出异常
        if (!response.getResult()) {
            throw new BusinessException(response.getMsg());
        }
    }


    /**
     * 处理参数
     *
     * @param databaseInfo
     * @param properties
     */
    private void handleProperties(DatabaseConnectionInfo databaseInfo, JSONObject properties) {

        if (StringUtils.isNotBlank(databaseInfo.getMode())) {
            properties.put(Constants.FTP_PROPERTIES.MODE, databaseInfo.getMode());
        }

        if (StringUtils.isNotBlank(databaseInfo.getControlEncoding())) {
            properties.put(Constants.FTP_PROPERTIES.CONTROL_ENCODING, databaseInfo.getControlEncoding());
        }

//        String properties = form.getProperties();

//        String extConfig = form.getExtConfig();

//        if (null != form.getUseSSL()) {
//            map.put("useSSL", form.getUseSSL());
//        }
//        if (null != form.getUseCopy()) {
//            map.put("useCopy", form.getUseCopy());
//        }
//        if (StringUtils.isNotBlank(form.getAuthWay())) {
//            map.put("authWay", form.getAuthWay());
//        }
//        if (null != form.getConnectionTimeout()) {
//            map.put("connectionTimeout", form.getConnectionTimeout());
//        }
//        if (null != form.getReadTimeout()) {
//            map.put("readTimeout", form.getReadTimeout());
//        }
//        if (StringUtils.isNotBlank(form.getControlEncoding())) {
//            map.put("controlEncoding", form.getControlEncoding());
//        }
//        if (StringUtils.isNotBlank(form.getMode())) {
//            map.put("mode", form.getMode());
//        }
//        if (StringUtils.isNotBlank(form.getSecretId())) {
//            map.put("secretId", form.getSecretId());
//        }
//        if (StringUtils.isNotBlank(form.getSecretKey())) {
//            map.put("secretKey", form.getSecretKey());
//        }
//        if (StringUtils.isNotBlank(form.getOdpsEndpoint())) {
//            map.put("odpsEndpoint", form.getOdpsEndpoint());
//        }
//        if (StringUtils.isNotBlank(form.getTunnelEndpoint())) {
//            map.put("tunnelEndpoint", form.getTunnelEndpoint());
//        }
//        if (StringUtils.isNotBlank(form.getProject())) {
//            map.put("project", form.getProject());
//        }
//        if (StringUtils.isNotBlank(form.getAccessKeyId())) {
//            map.put("accessKeyId", form.getAccessKeyId());
//        }
//        if (StringUtils.isNotBlank(form.getAccessKeySecret())) {
//            map.put("accessKeySecret", form.getAccessKeySecret());
//        }
//        if (Objects.nonNull(form.getPropertiesList())) {
//            for (PropertiesForm propertiesForm : form.getPropertiesList()) {
//                map.put(propertiesForm.getName(), propertiesForm.getValue());
//            }
//        }
    }

    /**
     * 处理扩展配置
     *
     * @param databaseInfo
     * @param extConfig
     */
    private void handExtConfig(DatabaseConnectionInfo databaseInfo, JSONObject extConfig) {
        if (StringUtils.isNotBlank(databaseInfo.getFeAddress())) {
            extConfig.put(Constants.DATABASE_EXT_CONFIG.FE_ADDRESS, databaseInfo.getFeAddress());
        }

        if (StringUtils.isNotBlank(databaseInfo.getBeAddress())) {
            extConfig.put(Constants.DATABASE_EXT_CONFIG.BE_ADDRESS, databaseInfo.getBeAddress());
        }
    }

    /**
     * 连接FTP/FTPS
     *
     * @param databaseConnectionInfo
     */
    private void connectFTP(DatabaseConnectionInfo databaseConnectionInfo) {
        AbstractFTPConnector connector = FTPConnectorFactory.getConnector(databaseConnectionInfo.getType());
        connector.setHost(databaseConnectionInfo.getDbHost());
        connector.setPort(Integer.parseInt(databaseConnectionInfo.getDbPort()));
        connector.setUsername(databaseConnectionInfo.getUsername());
        connector.setPassword(databaseConnectionInfo.getPassword());
        connector.setMode(databaseConnectionInfo.getMode());
        connector.setControlEncoding(databaseConnectionInfo.getControlEncoding());
        connector.connect();
    }

    /**
     * 校验数据源名称是否存在
     *
     * @param name
     */
    private void verifyDbNameExisted(String name, Integer groupId) {
        LambdaQueryWrapper<DbDatabase> ldq = new LambdaQueryWrapper<>();
        ldq.eq(DbDatabase::getDbName, name).eq(DbDatabase::getGroupId, groupId).eq(DbDatabase::getDeleted, Constants.DELETE_FLAG.FALSE);
        DbDatabase database = dbDatabaseDao.selectOne(ldq);
        if (Objects.nonNull(database)) {
            throw new BusinessException("数据源已存在!");
        }
    }
}

