package com.pufferfishscheduler.master.collect.rule.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.codex.java.JavaClassBodyExecutor;
import com.pufferfishscheduler.common.codex.java.Param;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.condition.SqlConditionBuilder;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.node.GenericTreeBuilder;
import com.pufferfishscheduler.common.node.Tree;
import com.pufferfishscheduler.common.node.TreeNode;
import com.pufferfishscheduler.common.utils.*;
import com.pufferfishscheduler.dao.entity.DbDatabase;
import com.pufferfishscheduler.dao.entity.Rule;
import com.pufferfishscheduler.dao.entity.RuleGroup;
import com.pufferfishscheduler.dao.entity.RuleProcessor;
import com.pufferfishscheduler.dao.mapper.RuleGroupMapper;
import com.pufferfishscheduler.dao.mapper.RuleMapper;
import com.pufferfishscheduler.dao.mapper.RuleProcessorMapper;
import com.pufferfishscheduler.domain.vo.collect.RuleInformationVo;
import com.pufferfishscheduler.domain.form.collect.RuleForm;
import com.pufferfishscheduler.domain.model.database.DatabaseConnectionInfo;
import com.pufferfishscheduler.domain.vo.collect.RuleDetailVo;
import com.pufferfishscheduler.domain.vo.collect.RuleVo;
import com.pufferfishscheduler.master.collect.rule.service.RuleService;
import com.pufferfishscheduler.master.database.database.service.DbDatabaseService;
import com.pufferfishscheduler.master.upms.service.UserAdminService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * 规则服务实现
 *
 * 说明：
 * - 去掉工作空间校验
 * - 去掉管理员权限校验
 * - 不包含标准库值映射逻辑
 */
@Slf4j
@Service("ruleService")
public class RuleServiceImpl implements RuleService {

    @Autowired
    private RuleMapper ruleMapper;

    @Autowired
    private RuleGroupMapper groupMapper;

    @Autowired
    private RuleProcessorMapper processorMapper;

    @Autowired
    private GenericTreeBuilder treeBuilder;

    @Autowired
    private UserAdminService  userAdminService;

    @Autowired
    private DbDatabaseService dbDatabaseService;

    @Autowired
    private AESUtil aesUtil;

    @Value("${rule.group.generic-id:}")
    private String genericGroupIdCfg;

    @Value("${rule.group.public-id:}")
    private String publicGroupIdCfg;

    @Value("${rule.group.custom-id:}")
    private String customGroupIdCfg;

    @Override
    public IPage<RuleVo> list(Integer groupId, String ruleName, Integer pageNo, Integer pageSize) {
        LambdaQueryWrapper<Rule> qw = new LambdaQueryWrapper<>();
        qw.eq(Rule::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(Rule::getUpdatedTime)
                .orderByDesc(Rule::getCreatedTime);


        if (groupId != null) {
            List<Integer> allGroupIds = getAllChildGroupIds(groupId);
            if (allGroupIds.isEmpty()) {
                qw.eq(Rule::getGroupId, groupId);
            } else {
                qw.in(Rule::getGroupId, allGroupIds);
            }
        }

        if (StringUtils.isNotBlank(ruleName)) {
            qw.like(Rule::getRuleName, ruleName.trim());
        }

        Page<Rule> page = ruleMapper.selectPage(new Page<>(pageNo, pageSize), qw);
        Page<RuleVo> out = new Page<>(page.getCurrent(), page.getSize(), page.getTotal());
        if (page.getRecords().isEmpty()) {
            return out;
        }

        Map<Integer, RuleGroup> groupMap = getGroupMap();
        Map<Integer, RuleProcessor> processorMap = getProcessorMap();

        List<RuleVo> records = page.getRecords().stream().map(r -> toRuleVo(r, groupMap, processorMap)).collect(Collectors.toList());
        out.setRecords(records);
        return out;
    }

    /**
     * 规则详情
     * @param id 规则ID
     * @return 规则详情
     */
    @Override
    public RuleDetailVo detail(String id) {
        Rule rule = ruleMapper.selectById(id);
        if (rule == null || Boolean.TRUE.equals(rule.getDeleted())) {
            throw new BusinessException("规则不存在，请刷新重试！");
        }
        Map<Integer, RuleGroup> groupMap = getGroupMap();
        Map<Integer, RuleProcessor> processorMap = getProcessorMap();

        RuleVo base = toRuleVo(rule, groupMap, processorMap);
        RuleDetailVo vo = new RuleDetailVo();
        vo.setId(base.getId());
        vo.setGroupId(base.getGroupId());
        vo.setGroupName(base.getGroupName());
        vo.setFirstGroupId(base.getFirstGroupId());
        vo.setRuleType(base.getRuleType());
        vo.setRuleTypeTxt(base.getRuleTypeTxt());
        vo.setRuleCode(base.getRuleCode());
        vo.setRuleName(base.getRuleName());
        vo.setRuleDescription(base.getRuleDescription());
        vo.setRuleProcessorId(base.getRuleProcessorId());
        vo.setProcessorName(base.getProcessorName());
        vo.setStatus(base.getStatus());
        vo.setStatusTxt(base.getStatusTxt());
        vo.setDeleted(base.getDeleted());
        vo.setCreatedBy(base.getCreatedBy());
        vo.setCreatedTime(base.getCreatedTime());
        vo.setCreatedTimeTxt(base.getCreatedTimeTxt());
        vo.setUpdatedBy(base.getUpdatedBy());
        vo.setUpdatedTime(base.getUpdatedTime());
        vo.setUpdatedTimeTxt(base.getUpdatedTimeTxt());
        vo.setConfig(rule.getConfig());
        return vo;
    }

    /**
     * 新增规则
     * @param form 规则表单
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(RuleForm form) {
        RuleGroup group = requireGroup(form.getGroupId());
        validateRuleCreatePermission(group);
        verifyRuleNameUnique(form.getRuleName(), form.getGroupId(), null);

        Rule rule = new Rule();
        rule.setId(CommonUtil.getUUIDString());
        rule.setGroupId(form.getGroupId());
        rule.setRuleCode("GZ_" + System.currentTimeMillis());
        rule.setRuleName(form.getRuleName());
        rule.setRuleDescription(form.getRuleDescription());
        rule.setRuleProcessorId(form.getRuleProcessorId());
        rule.setRuleType(resolveRuleType(group));
        rule.setFirstGroupId(resolveFirstGroupId(group));
        rule.setStatus(form.getStatus() == null ? Boolean.FALSE : form.getStatus());
        rule.setConfig(form.getConfig());
        rule.setDeleted(Constants.DELETE_FLAG.FALSE);
        rule.setCreatedBy(UserContext.getCurrentAccount());
        rule.setCreatedTime(new Date());
        rule.setUpdatedBy(UserContext.getCurrentAccount());
        rule.setUpdatedTime(new Date());
        ruleMapper.insert(rule);
    }

    /**
     * 编辑规则
     * @param form 规则表单
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(RuleForm form) {
        Rule existing = ruleMapper.selectById(form.getId());
        if (existing == null || Boolean.TRUE.equals(existing.getDeleted())) {
            throw new BusinessException("规则不存在，请刷新重试！");
        }
        if (isGenericRule(existing)) {
            throw new BusinessException("通用规则不允许编辑！");
        }

        RuleGroup group = requireGroup(form.getGroupId());
        verifyRuleNameUnique(form.getRuleName(), form.getGroupId(), form.getId());

        Rule toUpdate = new Rule();
        toUpdate.setId(existing.getId());
        toUpdate.setGroupId(form.getGroupId());
        toUpdate.setRuleName(form.getRuleName());
        toUpdate.setRuleCode(StringUtils.defaultIfBlank(form.getRuleCode(), existing.getRuleCode()));
        toUpdate.setRuleDescription(form.getRuleDescription());
        toUpdate.setRuleProcessorId(form.getRuleProcessorId());
        toUpdate.setRuleType(resolveRuleType(group));
        toUpdate.setFirstGroupId(resolveFirstGroupId(group));
        toUpdate.setStatus(form.getStatus() == null ? existing.getStatus() : form.getStatus());
        toUpdate.setConfig(form.getConfig());
        toUpdate.setUpdatedBy(UserContext.getCurrentAccount());
        toUpdate.setUpdatedTime(new Date());
        ruleMapper.updateById(toUpdate);
    }

    /**
     * 逻辑删除规则
     * @param id 规则ID
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(String id) {
        Rule existing = ruleMapper.selectById(id);
        if (existing == null || Boolean.TRUE.equals(existing.getDeleted())) {
            throw new BusinessException("规则不存在，请刷新重试！");
        }
        if (isGenericRule(existing)) {
            throw new BusinessException("通用规则不允许删除！");
        }
        Rule toUpdate = new Rule();
        toUpdate.setId(existing.getId());
        toUpdate.setDeleted(Constants.DELETE_FLAG.TRUE);
        toUpdate.setUpdatedBy(UserContext.getCurrentAccount());
        toUpdate.setUpdatedTime(new Date());
        ruleMapper.updateById(toUpdate);
    }

    /**
     * 发布/禁用规则
     * @param id 规则ID
     * @param status 发布/禁用状态
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void release(String id, Boolean status) {
        Rule existing = ruleMapper.selectById(id);
        if (existing == null || Boolean.TRUE.equals(existing.getDeleted())) {
            throw new BusinessException("规则不存在，请刷新重试！");
        }
        Rule toUpdate = new Rule();
        toUpdate.setId(existing.getId());
        toUpdate.setStatus(status);
        toUpdate.setUpdatedBy(UserContext.getCurrentAccount());
        toUpdate.setUpdatedTime(new Date());
        ruleMapper.updateById(toUpdate);
    }

    /**
     * 分组+规则树（只包含已发布规则）
     * @return
     */
    @Override
    public List<Tree> tree() {
        List<RuleGroup> groups = groupMapper.selectList(new LambdaQueryWrapper<RuleGroup>()
                .eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(RuleGroup::getOrderBy)
                .orderByAsc(RuleGroup::getId));

        List<Rule> rules = ruleMapper.selectList(new LambdaQueryWrapper<Rule>()
                .eq(Rule::getDeleted, Constants.DELETE_FLAG.FALSE)
                .eq(Rule::getStatus, Boolean.TRUE)
                .orderByDesc(Rule::getUpdatedTime)
                .orderByAsc(Rule::getRuleName));
        List<Tree> allNodes = new ArrayList<>();
        for (RuleGroup g : groups) {
            Tree node = new Tree();
            node.setId(g.getId());
            node.setName(g.getGroupName());
            node.setType(Constants.TREE_TYPE.GROUP);
            node.setParentId(g.getParentId());
            node.setOrderBy(g.getOrderBy());
            allNodes.add(node);
        }
        for (Rule r : rules) {
            Tree node = new Tree();
            // 规则树ID使用规则主键，避免冲突概率极低
            node.setTreeId(r.getId());
            node.setName(r.getRuleName());
            node.setType(Constants.TREE_TYPE.RULE);
            node.setParentId(r.getGroupId());
            node.setOrderBy(0);
            allNodes.add(node);
        }
        return treeBuilder.buildTree(allNodes, Comparator.comparing(TreeNode::getOrder));
    }

    /**
     * 校验Java自定义规则脚本语法
     * @param json 包含规则脚本的JSON对象
     * @throws Exception 校验失败时抛出异常
     */
    @Override
    public void validateJavaCode(JSONObject json) {
        JSONObject data = getJavaCodeData(json);
        String javaCode = data.getString("javaCode");
        JavaClassBodyExecutor executor = new JavaClassBodyExecutor();
        try {
            executor.init(javaCode);
        } catch (Exception e) {
            log.error("[Java自定义]：Java脚本编译异常！", e);
            throw new BusinessException("[Java自定义]：Java脚本编译异常！");
        }
    }

    /**
     * 预览Java自定义规则结果
     * @param json 包含规则脚本的JSON对象
     * @return 预览结果
     */
    @Override
    public Object previewJavaCode(JSONObject json) {
        JSONObject data = getJavaCodeData(json);
        String javaCode = data.getString("javaCode");
        Object value = data.get("value");

        if (javaCode == null || javaCode.isBlank()) {
            throw new BusinessException("[Java自定义]：Java代码不能为空！");
        }
        if (value == null) {
            throw new BusinessException("[Java自定义]：测试值不能为空！");
        }

        try {
            JavaClassBodyExecutor executor = new JavaClassBodyExecutor();
            executor.init(javaCode);
            return executor.javaCustomRule(new Param(), value);
        } catch (Exception e) {
            log.error("[Java自定义] 脚本编译/执行异常", e);
            throw new BusinessException("[Java自定义]：脚本编译或执行异常！");
        }
    }

    /**
     * 校验sql脚本
     * @param json 包含sql脚本的JSON对象
     * @throws Exception 校验失败时抛出异常
     */
    @Override
    public void validateSql(JSONObject json) {
        Integer processorId = json.getInteger("ruleProcessorId");
        if (!Constants.PROCESSOR_ID.VALUE_MAPPING.equals(processorId)) {
            throw new BusinessException("[值映射]：该规则不是值映射规则类型！");
        }

        JSONObject data = getConfigData(json);
        Integer dataSourceId = data.getInteger("dataSourceId");
        String sql = data.getString("sqlContext");

        if (sql == null || sql.isBlank()) {
            throw new BusinessException("[值映射]：SQL语句不能为空！");
        }

        // SQL语法校验（双列、禁止*）
        SqlConditionBuilder.validateCustomSql(sql);

        // 数据库连接校验
        DatabaseConnectionInfo connectionInfo = getDbConnectionInfo(dataSourceId);
        SqlConditionBuilder.validateSqlSyntax(sql, connectionInfo);
    }

    /**
     * @param json 包含sql脚本的JSON对象
     * @return 预览结果
     */
    @Override
    public Map<String, Object> preview(JSONObject json) {
        Integer processorId = json.getInteger("ruleProcessorId");
        if (!Constants.PROCESSOR_ID.VALUE_MAPPING.equals(processorId)) {
            throw new BusinessException("[值映射]：该规则不是值映射规则类型！");
        }

        JSONObject data = getConfigData(json);
        Integer dataSourceId = data.getInteger("dataSourceId");
        Integer mappingType = data.getInteger("mappingType");

        if (mappingType == null) {
            throw new BusinessException("[值映射]：请选择映射方式！");
        }

        DatabaseConnectionInfo connectionInfo = getDbConnectionInfo(dataSourceId);
        return getPreviewData(data, connectionInfo, mappingType);
    }

    /**
     * 获取规则信息(已发布)
     */
    @Override
    public List<RuleInformationVo> getRuleInformation(JSONArray ruleIds) {
        if (ruleIds == null || ruleIds.isEmpty()) {
            return List.of();
        }

        List<String> ruleIdList = ruleIds.toList(String.class);

        List<RuleInformationVo> list = ruleMapper.getRuleInformation(ruleIdList);
        if (list == null || list.isEmpty()) {
            return List.of();
        }

        Map<Integer, DatabaseConnectionInfo> dataSourceCache = new HashMap<>();
        for (RuleInformationVo vo : list) {
            // 仅处理值映射类型
            if (!Objects.equals(vo.getProcessorId(), Constants.PROCESSOR_ID.VALUE_MAPPING)) {
                continue;
            }

            JSONObject data = parseRuleInformationData(vo.getConfig());
            Integer mappingType = data.getInteger("mappingType");
            if (mappingType == null) {
                throw new BusinessException("校验是否选择数据源或者映射方式！");
            }

            JSONObject mappedData = new JSONObject();
            if (Objects.equals(mappingType, Constants.MAPPING_TYPE.MANUAL)) {
                mappedData.put("fieldList", data.getJSONArray("fieldList"));
            } else if (Objects.equals(mappingType, Constants.MAPPING_TYPE.DATABASE_DICT_TABLE)) {
                appendDatabaseDictData(mappedData, data, dataSourceCache);
            } else if (Objects.equals(mappingType, Constants.MAPPING_TYPE.CUSTOM_SQL)) {
                appendCustomSqlData(mappedData, data, dataSourceCache);
            }

            appendCommonMappingData(mappedData, data, mappingType);
            JSONObject newConfig = new JSONObject();
            newConfig.put("data", mappedData);
            vo.setConfig(newConfig.toString());
        }

        return list;
    }

    private JSONObject parseRuleInformationData(String configText) {
        JSONObject config = JSONObject.parseObject(configText);
        if (config == null) {
            throw new BusinessException("请校验参数配置！");
        }
        JSONObject data = config.getJSONObject("data");
        if (data == null) {
            throw new BusinessException("请校验参数配置！");
        }
        return data;
    }

    /**
     * 追加数据库字典表映射数据
     * @param target 目标JSON对象
     * @param data   原始JSON对象
     * @param dataSourceCache 数据源缓存
     */
    private void appendDatabaseDictData(JSONObject target, JSONObject data, Map<Integer, DatabaseConnectionInfo> dataSourceCache) {
        Integer dataSourceId = data.getInteger("dataSourceId");
        DatabaseConnectionInfo connectionInfo = getConnectionInfoFromCache(dataSourceId, dataSourceCache);
        appendConnectionInfo(target, connectionInfo);
        target.put("tableName", data.getString("tableName"));
        target.put("beforefieldName", data.getString("beforefieldName"));
        target.put("afterfieldName", data.getString("afterfieldName"));
        target.put("condition", data.getJSONObject("condition"));
    }

    /**
     * 追加自定义SQL映射数据
     * @param target 目标JSON对象
     * @param data   原始JSON对象
     * @param dataSourceCache 数据源缓存
     */
    private void appendCustomSqlData(JSONObject target, JSONObject data, Map<Integer, DatabaseConnectionInfo> dataSourceCache) {
        Integer dataSourceId = data.getInteger("dataSourceId");
        DatabaseConnectionInfo connectionInfo = getConnectionInfoFromCache(dataSourceId, dataSourceCache);
        appendConnectionInfo(target, connectionInfo);
        target.put("sql", data.getString("sqlContext"));
    }

    /**
     * 从缓存中获取数据库连接信息
     * @param dataSourceId 数据源ID
     * @param dataSourceCache 数据源缓存
     * @return 数据库连接信息
     */
    private DatabaseConnectionInfo getConnectionInfoFromCache(Integer dataSourceId, Map<Integer, DatabaseConnectionInfo> dataSourceCache) {
        if (dataSourceId == null) {
            throw new BusinessException("校验是否选择数据源或者映射方式！");
        }
        return dataSourceCache.computeIfAbsent(dataSourceId, dbDatabaseService::getDatabaseConnectionInfo);
    }

    /**
     * 追加数据库连接信息
     * @param target 目标JSON对象
     * @param connectionInfo 数据库连接信息
     */
    private void appendConnectionInfo(JSONObject target, DatabaseConnectionInfo connectionInfo) {
        if (connectionInfo == null) {
            throw new BusinessException("数据源不存在！");
        }
        target.put("host", connectionInfo.getDbHost());
        target.put("port", connectionInfo.getDbPort());
        target.put("username", connectionInfo.getUsername());
        target.put("password", connectionInfo.getPassword());
        target.put("dbName", connectionInfo.getDbName());
        target.put("dbType", connectionInfo.getType());
        if (connectionInfo.getDbSchema() != null) {
            target.put("schema", connectionInfo.getDbSchema());
        }
        if (connectionInfo.getProperties() != null) {
            target.put("properties", connectionInfo.getProperties());
        }
    }

    /**
     * 追加通用映射数据
     * @param target 目标JSON对象
     * @param source 原始JSON对象
     * @param mappingType 映射类型
     */
    private void appendCommonMappingData(JSONObject target, JSONObject source, Integer mappingType) {
        target.put("mappingType", mappingType);
        target.put("rename", source.getString("rename"));
        target.put("renameType", source.getString("renameType"));
        target.put("fieldName", source.getString("fieldName"));
        target.put("fieldType", source.getString("fieldType"));
    }

    /**
     * 统一获取 config 中的 data 对象（抽成公共方法，消除重复）
     */
    private JSONObject getConfigData(JSONObject json) {
        if (!json.containsKey("config")) {
            throw new BusinessException("请传输参数配置！");
        }
        JSONObject config = json.getJSONObject("config");

        if (!config.containsKey("data")) {
            throw new BusinessException("请校验参数配置！");
        }
        return config.getJSONObject("data");
    }

    /**
     * 获取Java代码配置（已精简）
     */
    private JSONObject getJavaCodeData(JSONObject json) {
        Integer processorId = json.getInteger("ruleProcessorId");
        if (!Constants.PROCESSOR_ID.JAVA_CUSTOM.equals(processorId)) {
            throw new BusinessException("[Java自定义]：该规则不是Java自定义规则类型！");
        }
        return getConfigData(json);
    }

    /**
     * 统一获取并解密数据库连接信息
     */
    private DatabaseConnectionInfo getDbConnectionInfo(Integer dataSourceId) {
        if (dataSourceId == null) {
            throw new BusinessException("请选择数据源！");
        }

        DbDatabase database = dbDatabaseService.getDatabaseById(dataSourceId);
        if (database == null) {
            throw new BusinessException("数据源不存在！");
        }

        DatabaseConnectionInfo connectionInfo = new DatabaseConnectionInfo();
        BeanUtils.copyProperties(database, connectionInfo);
        connectionInfo.setPassword(aesUtil.decrypt(database.getPassword()));

        return connectionInfo;
    }

    /**
     * 处理预览数据集合
     * @param data 前端传入配置
     * @param connectionInfo 数据库连接信息
     * @param mappingType 映射类型 1-可视化配置 2-自定义SQL
     * @return 预览结果（fieldList + dataList）
     */
    private Map<String, Object> getPreviewData(JSONObject data, DatabaseConnectionInfo connectionInfo, Integer mappingType) {
        String dbType = connectionInfo.getType();
        Map<String, Object> result = new HashMap<>();
        String sql;

        try {
            // 1 构建基础SQL
            if (mappingType == 1) {
                // 可视化配置 → 直接用工具类构建
                String beforeFieldName = data.getString("beforefieldName");
                String afterFieldName = data.getString("afterfieldName");
                String tableName = data.getString("tableName");

                // 构建带schema的表名 → 复用工具类
                tableName = SqlConditionBuilder.buildFullTableName(dbType, connectionInfo.getDbSchema(),connectionInfo.getDbName(), tableName);

                // 构建WHERE条件 → 工具类
                String whereClause = "";
                JSONObject condition = data.getJSONObject("condition");
                if (condition != null) {
                    whereClause = SqlConditionBuilder.buildWhereClause(condition, dbType);
                }

                // 拼接查询SQL（只查2列）
                sql = String.format("SELECT %s, %s FROM %s %s",
                        beforeFieldName, afterFieldName, tableName, whereClause);
            } else {
                // 自定义SQL → 工具类校验 + 清理
                String customSql = data.getString("sqlContext");
                SqlConditionBuilder.validateCustomSql(customSql);
                sql = SqlConditionBuilder.cleanPreviewSql(customSql);
            }

            // 2 统一分页（预览只查1条）→ 工具类
            sql = SqlConditionBuilder.buildPreviewLimitSql(sql, dbType);

            // 3 执行查询
            try (Connection conn = JdbcUtil.getConnection(
                    JdbcUrlUtil.getDriver(dbType),
                    JdbcUrlUtil.getUrl(dbType, connectionInfo.getDbHost(), connectionInfo.getDbPort(), connectionInfo.getDbName(), connectionInfo.getProperties()),
                    connectionInfo);
                 Statement stat = conn.createStatement();
                 ResultSet rs = stat.executeQuery(sql)) {

                ResultSetMetaData rsmd = rs.getMetaData();
                // 结果映射 → 工具类
                SqlConditionBuilder.mapPreviewResult(result, rs, rsmd);
            }

        } catch (SQLException e) {
            log.error("[数据预览异常]:{}数据库操作异常：{}", dbType, e.getMessage());
            throw new BusinessException("[数据预览异常]:" + dbType + "数据库操作异常：" + e.getMessage());
        }

        return result;
    }

    /**
     * 校验分类是否存在
     * @param groupId 分类ID
     * @return 分类对象
     */
    private RuleGroup requireGroup(Integer groupId) {
        RuleGroup group = groupMapper.selectById(groupId);
        if (group == null || Boolean.TRUE.equals(group.getDeleted())) {
            throw new BusinessException("请校验该分类是否存在！");
        }
        return group;
    }

    /**
     * 解析顶层分类ID
     * @param group 分类对象
     * @return 顶层分类ID
     */
    private Integer resolveFirstGroupId(RuleGroup group) {
        RuleGroup current = group;
        while (current.getParentId() != null) {
            RuleGroup parent = groupMapper.selectById(current.getParentId());
            if (parent == null || Boolean.TRUE.equals(parent.getDeleted())) {
                break;
            }
            current = parent;
        }
        return current.getId();
    }

    /**
     * 解析规则类型
     * @param group 分类对象
     * @return 规则类型
     */
    private Integer resolveRuleType(RuleGroup group) {
        return resolveFirstGroupId(group);
    }

    /**
     * 校验规则名称是否唯一
     * @param ruleName 规则名称
     * @param groupId 分类ID
     * @param excludeId 排除的规则ID（可选）
     */
    private void verifyRuleNameUnique(String ruleName, Integer groupId, String excludeId) {
        LambdaQueryWrapper<Rule> qw = new LambdaQueryWrapper<>();
        qw.eq(Rule::getRuleName, ruleName)
                .eq(Rule::getGroupId, groupId)
                .eq(Rule::getDeleted, Constants.DELETE_FLAG.FALSE);
        if (StringUtils.isNotBlank(excludeId)) {
            qw.ne(Rule::getId, excludeId);
        }
        Long cnt = ruleMapper.selectCount(qw);
        if (cnt != null && cnt > 0) {
            throw new BusinessException("同级分类下规则名称不能重复!");
        }
    }

    /**
     * 获取所有分类映射
     * @return 分类映射
     */
    private Map<Integer, RuleGroup> getGroupMap() {
        return groupMapper.selectList(new LambdaQueryWrapper<RuleGroup>().eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE))
                .stream().collect(Collectors.toMap(RuleGroup::getId, g -> g, (a, b) -> a));
    }

    /**
     * 获取所有规则处理器映射
     * @return 处理器映射
     */
    private Map<Integer, RuleProcessor> getProcessorMap() {
        return processorMapper.selectList(new LambdaQueryWrapper<>())
                .stream().collect(Collectors.toMap(RuleProcessor::getId, p -> p, (a, b) -> a));
    }

    /**
     * 转换为VO
     * @param r 规则对象
     * @param groupMap 分类映射
     * @param processorMap 处理器映射
     * @return 规则VO
     */
    private RuleVo toRuleVo(Rule r, Map<Integer, RuleGroup> groupMap, Map<Integer, RuleProcessor> processorMap) {
        RuleVo vo = new RuleVo();
        vo.setId(r.getId());
        vo.setGroupId(r.getGroupId());
        vo.setFirstGroupId(r.getFirstGroupId());
        vo.setRuleType(r.getRuleType());
        vo.setRuleCode(r.getRuleCode());
        vo.setRuleName(r.getRuleName());
        vo.setRuleDescription(r.getRuleDescription());
        vo.setRuleProcessorId(r.getRuleProcessorId());
        vo.setStatus(r.getStatus());
        vo.setDeleted(r.getDeleted());
        vo.setCreatedBy(r.getCreatedBy());
        vo.setCreatedTime(r.getCreatedTime());
        vo.setCreatedTimeTxt(DateUtil.formatDateTime(r.getCreatedTime()));
        vo.setUpdatedBy(r.getUpdatedBy());
        vo.setUpdatedTime(r.getUpdatedTime());
        vo.setUpdatedTimeTxt(DateUtil.formatDateTime(r.getUpdatedTime()));
        RuleGroup group = groupMap.get(r.getGroupId());
        if (group != null) {
            vo.setGroupName(buildFullGroupName(group, groupMap));
        }
        RuleProcessor processor = processorMap.get(r.getRuleProcessorId());
        if (processor != null) {
            vo.setProcessorName(processor.getProcessorName());
        }
        vo.setRuleTypeTxt(resolveRuleTypeTxt(r.getRuleType()));
        vo.setStatusTxt(Boolean.TRUE.equals(r.getStatus()) ? "发布" : "草稿");
        return vo;
    }

    /**
     * 获取所有子分类ID
     * @param parentGroupId 父分类ID
     * @return 所有子分类ID
     */
    private List<Integer> getAllChildGroupIds(Integer parentGroupId) {
        List<RuleGroup> groups = groupMapper.selectList(new LambdaQueryWrapper<RuleGroup>()
                .eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE));
        Map<Integer, List<Integer>> parentChildrenMap = groups.stream()
                .filter(g -> g.getParentId() != null)
                .collect(Collectors.groupingBy(RuleGroup::getParentId,
                        Collectors.mapping(RuleGroup::getId, Collectors.toList())));
        LinkedHashSet<Integer> ids = new LinkedHashSet<>();
        ids.add(parentGroupId);
        collectChildIds(parentGroupId, parentChildrenMap, ids);
        return new ArrayList<>(ids);
    }

    /**
     * 递归收集所有子分类ID
     * @param parentId 父分类ID
     * @param parentChildrenMap 父子分类映射
     * @param out 输出集合
     */
    private void collectChildIds(Integer parentId, Map<Integer, List<Integer>> parentChildrenMap, Set<Integer> out) {
        List<Integer> children = parentChildrenMap.get(parentId);
        if (children == null || children.isEmpty()) {
            return;
        }
        for (Integer childId : children) {
            if (out.add(childId)) {
                collectChildIds(childId, parentChildrenMap, out);
            }
        }
    }

    /**
     * 构建完整分类名称
     * @param group 分类对象
     * @param groupMap 分类映射
     * @return 完整分类名称
     */
    private String buildFullGroupName(RuleGroup group, Map<Integer, RuleGroup> groupMap) {
        List<String> names = new ArrayList<>();
        RuleGroup current = group;
        while (current != null) {
            names.add(current.getGroupName());
            current = current.getParentId() == null ? null : groupMap.get(current.getParentId());
        }
        Collections.reverse(names);
        return String.join("/", names);
    }

    /**
     * 解析规则类型文本
     * @param ruleType 规则类型
     * @return 规则类型文本
     */
    private String resolveRuleTypeTxt(Integer ruleType) {
        if (ruleType == null) {
            return "";
        }
        return "类型-" + ruleType;
    }

    /**
     * 验证规则创建权限
     * @param selectedGroup 选中的分类对象
     */
    private void validateRuleCreatePermission(RuleGroup selectedGroup) {
        Integer firstGroupId = resolveFirstGroupId(selectedGroup);
        Integer genericId = resolveGenericGroupId();
        Integer publicId = resolvePublicGroupId();
        Integer customId = resolveCustomGroupId();

        if (genericId != null && Objects.equals(firstGroupId, genericId)) {
            throw new BusinessException("通用目录下不允许新建规则！");
        }
        if (userAdminService.validateIsAdmin()) {
            if (publicId != null && customId != null
                    && !Objects.equals(firstGroupId, publicId)
                    && !Objects.equals(firstGroupId, customId)) {
                throw new BusinessException("管理员仅可在公共/自定义目录下新建规则！");
            }
            return;
        }
        if (customId == null || !Objects.equals(firstGroupId, customId)) {
            throw new BusinessException("非管理员仅可在自定义目录及其子级下新建规则！");
        }
    }

    /**
     * 解析通用目录ID
     * @return 通用目录ID
     */
    private Integer resolveGenericGroupId() {
        Integer fromCfg = parseIntOrNull(genericGroupIdCfg);
        if (fromCfg != null) {
            return fromCfg;
        }
        return findTopGroupIdByKeyword("通用");
    }

    /**
     * 解析公共目录ID
     * @return 公共目录ID
     */
    private Integer resolvePublicGroupId() {
        Integer fromCfg = parseIntOrNull(publicGroupIdCfg);
        if (fromCfg != null) {
            return fromCfg;
        }
        return findTopGroupIdByKeyword("公共");
    }

    /**
     * 解析自定义目录ID
     * @return 自定义目录ID
     */
    private Integer resolveCustomGroupId() {
        Integer fromCfg = parseIntOrNull(customGroupIdCfg);
        if (fromCfg != null) {
            return fromCfg;
        }
        return findTopGroupIdByKeyword("自定义");
    }

    /**
     * 根据关键词查找顶部分类ID
     * @param keyword 关键词
     * @return 顶部分类ID
     */
    private Integer findTopGroupIdByKeyword(String keyword) {
        LambdaQueryWrapper<RuleGroup> qw = new LambdaQueryWrapper<>();
        qw.eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .isNull(RuleGroup::getParentId);
        List<RuleGroup> roots = groupMapper.selectList(qw);
        for (RuleGroup root : roots) {
            if (root.getGroupName() != null && root.getGroupName().contains(keyword)) {
                return root.getId();
            }
        }
        return null;
    }

    /**
     * 解析整数或返回null
     * @param s 字符串
     * @return 整数或null
     */
    private Integer parseIntOrNull(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private boolean isGenericRule(Rule rule) {
        Integer genericId = resolveGenericGroupId();
        if (genericId == null) {
            return false;
        }
        if (Objects.equals(rule.getFirstGroupId(), genericId)) {
            return true;
        }
        if (rule.getGroupId() == null) {
            return false;
        }
        RuleGroup group = groupMapper.selectById(rule.getGroupId());
        if (group == null || Boolean.TRUE.equals(group.getDeleted())) {
            return false;
        }
        return Objects.equals(resolveFirstGroupId(group), genericId);
    }
}
