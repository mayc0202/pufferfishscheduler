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
import com.pufferfishscheduler.domain.model.database.DBConnectionInfo;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 规则服务实现类
 *
 * 功能模块：
 * 1. 规则CRUD操作
 * 2. 规则分组管理
 * 3. 规则验证（Java/SQL）
 * 4. 规则预览
 * 5. 规则树构建
 *
 * @author mayc
 */
@Slf4j
@Service("ruleService")
public class RuleServiceImpl implements RuleService {

    private static final int DEFAULT_PREVIEW_LIMIT = 1;
    private static final String RULE_CODE_PREFIX = "GZ_";
    private static final String STATUS_PUBLISHED = "发布";
    private static final String STATUS_DRAFT = "草稿";
    private static final String TYPE_PREFIX = "类型-";

    // 映射类型常量
    private static final int MAPPING_TYPE_DICT = 0;
    private static final int MAPPING_TYPE_VISUAL = 1;
    private static final int MAPPING_TYPE_CUSTOM_SQL = 2;

    // 处理器类型常量
    private static final int PROCESSOR_VALUE_MAPPING = Constants.PROCESSOR_ID.VALUE_MAPPING;
    private static final int PROCESSOR_JAVA_CUSTOM = Constants.PROCESSOR_ID.JAVA_CUSTOM;

    @Autowired
    private RuleMapper ruleMapper;

    @Autowired
    private RuleGroupMapper groupMapper;

    @Autowired
    private RuleProcessorMapper processorMapper;

    @Autowired
    private GenericTreeBuilder treeBuilder;

    @Autowired
    private UserAdminService userAdminService;

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

    /**
     * 分页查询规则列表
     *
     * @param groupId  分组ID
     * @param ruleName 规则名称
     * @param pageNo   页码
     * @param pageSize 每页数量
     * @return 分页结果
     */
    @Override
    public IPage<RuleVo> list(Integer groupId, String ruleName, Integer pageNo, Integer pageSize) {
        LambdaQueryWrapper<Rule> queryWrapper = buildRuleListQuery(groupId, ruleName);
        Page<Rule> page = ruleMapper.selectPage(new Page<>(pageNo, pageSize), queryWrapper);

        if (page.getRecords().isEmpty()) {
            return new Page<>(page.getCurrent(), page.getSize(), page.getTotal());
        }

        Map<Integer, RuleGroup> groupMap = getGroupMap();
        Map<Integer, RuleProcessor> processorMap = getProcessorMap();

        List<RuleVo> records = page.getRecords().stream()
                .map(rule -> convertToRuleVo(rule, groupMap, processorMap))
                .collect(Collectors.toList());

        Page<RuleVo> result = new Page<>(page.getCurrent(), page.getSize(), page.getTotal());
        result.setRecords(records);
        return result;
    }

    /**
     * 查询规则详情
     *
     * @param id 规则ID
     * @return 规则详情
     */
    @Override
    public RuleDetailVo detail(String id) {
        Rule rule = getRuleById(id);
        Map<Integer, RuleGroup> groupMap = getGroupMap();
        Map<Integer, RuleProcessor> processorMap = getProcessorMap();

        RuleVo baseVo = convertToRuleVo(rule, groupMap, processorMap);
        RuleDetailVo detailVo = convertToDetailVo(baseVo);
        detailVo.setConfig(rule.getConfig());

        return detailVo;
    }

    /**
     * 查询规则基本信息
     *
     * @param ruleIds 规则ID列表
     * @return 规则基本信息列表
     */
    @Override
    public List<RuleInformationVo> getRuleInformation(JSONArray ruleIds) {
        if (ruleIds == null || ruleIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> ruleIdList = ruleIds.toList(String.class);
        List<RuleInformationVo> ruleInfoList = ruleMapper.getRuleInformation(ruleIdList);

        if (ruleInfoList == null || ruleInfoList.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Integer, DBConnectionInfo> dataSourceCache = new HashMap<>();
        for (RuleInformationVo vo : ruleInfoList) {
            enrichRuleInformation(vo, dataSourceCache);
        }

        return ruleInfoList;
    }

    /**
     * 构建规则树
     *
     * @return 规则树节点列表
     */
    @Override
    public List<Tree> tree() {
        List<RuleGroup> groups = getActiveGroups();
        List<Rule> rules = getPublishedRules();

        List<Tree> allNodes = new ArrayList<>();
        allNodes.addAll(convertGroupsToTreeNodes(groups));
        allNodes.addAll(convertRulesToTreeNodes(rules));

        return treeBuilder.buildTree(allNodes, Comparator.comparing(TreeNode::getOrder));
    }

    /**
     * 添加规则
     *
     * @param form 规则表单
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void add(RuleForm form) {
        RuleGroup group = getAndValidateGroup(form.getGroupId());
        validateRuleCreatePermission(group);
        validateRuleNameUniqueness(form.getRuleName(), form.getGroupId(), null);

        Rule rule = buildNewRule(form, group);
        ruleMapper.insert(rule);
    }

    /**
     * 更新规则
     *
     * @param form 规则表单
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void update(RuleForm form) {
        Rule existingRule = getRuleById(form.getId());
        validateModifiableRule(existingRule);

        RuleGroup group = getAndValidateGroup(form.getGroupId());
        validateRuleNameUniqueness(form.getRuleName(), form.getGroupId(), form.getId());

        Rule ruleToUpdate = buildUpdateRule(existingRule, form, group);
        ruleMapper.updateById(ruleToUpdate);
    }

    /**
     * 删除规则
     *
     * @param id 规则ID
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void delete(String id) {
        Rule existingRule = getRuleById(id);
        validateModifiableRule(existingRule);

        Rule ruleToDelete = buildDeleteRule(existingRule);
        ruleMapper.updateById(ruleToDelete);
    }

    /**
     * 发布/停用规则
     *
     * @param id 规则ID
     * @param status 发布/停用状态
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void release(String id, Boolean status) {
        Rule existingRule = getRuleById(id);

        Rule ruleToUpdate = new Rule();
        ruleToUpdate.setId(existingRule.getId());
        ruleToUpdate.setStatus(status);
        ruleToUpdate.setUpdatedBy(UserContext.getCurrentAccount());
        ruleToUpdate.setUpdatedTime(new Date());

        ruleMapper.updateById(ruleToUpdate);
    }

    /**
     * 验证Java脚本
     *
     * @param json 包含Java脚本的JSON对象
     */
    @Override
    public void validateJavaCode(JSONObject json) {
        String javaCode = extractAndValidateJavaCode(json);

        try {
            JavaClassBodyExecutor executor = new JavaClassBodyExecutor();
            executor.init(javaCode);
        } catch (Exception e) {
            log.error("[Java自定义] Java脚本编译异常", e);
            throw new BusinessException("[Java自定义]：Java脚本编译异常！");
        }
    }

    /**
     * 验证SQL语句
     *
     * @param json 包含SQL语句的JSON对象
     */
    @Override
    public void validateSql(JSONObject json) {
        validateProcessorType(json, PROCESSOR_VALUE_MAPPING, "值映射");

        JSONObject configData = extractConfigData(json);
        Integer dataSourceId = configData.getInteger("dataSourceId");
        String sql = configData.getString("sqlContext");

        validateNotEmpty(sql, "SQL语句");

        SqlConditionBuilder.validateCustomSql(sql);

        DBConnectionInfo connectionInfo = getDbConnectionInfo(dataSourceId);
        SqlConditionBuilder.validateSqlSyntax(sql, connectionInfo);
    }

    /**
     * 预览Java脚本
     *
     * @param json 包含Java脚本的JSON对象
     * @return 执行结果
     */
    @Override
    public Object previewJavaCode(JSONObject json) {
        String javaCode = extractAndValidateJavaCode(json);
        Object value = extractTestValue(json);

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
     * 预览SQL语句
     *
     * @param json 包含SQL语句的JSON对象
     * @return 执行结果
     */
    @Override
    public Map<String, Object> preview(JSONObject json) {
        validateProcessorType(json, PROCESSOR_VALUE_MAPPING, "值映射");

        JSONObject configData = extractConfigData(json);
        Integer dataSourceId = configData.getInteger("dataSourceId");
        Integer mappingType = configData.getInteger("mappingType");

        validateNotNull(mappingType, "映射方式");

        DBConnectionInfo connectionInfo = getDbConnectionInfo(dataSourceId);
        return executePreview(configData, connectionInfo, mappingType);
    }

    /**
     * 构建规则列表查询条件
     */
    private LambdaQueryWrapper<Rule> buildRuleListQuery(Integer groupId, String ruleName) {
        LambdaQueryWrapper<Rule> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(Rule::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(Rule::getUpdatedTime)
                .orderByDesc(Rule::getCreatedTime);

        if (groupId != null) {
            List<Integer> allGroupIds = getAllChildGroupIds(groupId);
            if (allGroupIds.isEmpty()) {
                queryWrapper.eq(Rule::getGroupId, groupId);
            } else {
                queryWrapper.in(Rule::getGroupId, allGroupIds);
            }
        }

        if (StringUtils.isNotBlank(ruleName)) {
            queryWrapper.like(Rule::getRuleName, ruleName.trim());
        }

        return queryWrapper;
    }

    /**
     * 获取所有子分组ID（包含自身）
     */
    private List<Integer> getAllChildGroupIds(Integer parentGroupId) {
        List<RuleGroup> groups = getActiveGroups();

        Map<Integer, List<Integer>> childrenMap = groups.stream()
                .filter(g -> g.getParentId() != null)
                .collect(Collectors.groupingBy(
                        RuleGroup::getParentId,
                        Collectors.mapping(RuleGroup::getId, Collectors.toList())
                ));

        Set<Integer> allIds = new LinkedHashSet<>();
        allIds.add(parentGroupId);
        collectChildIdsRecursively(parentGroupId, childrenMap, allIds);

        return new ArrayList<>(allIds);
    }

    /**
     * 递归收集子分组ID
     */
    private void collectChildIdsRecursively(Integer parentId, Map<Integer, List<Integer>> childrenMap, Set<Integer> out) {
        List<Integer> children = childrenMap.get(parentId);
        if (children == null || children.isEmpty()) {
            return;
        }

        for (Integer childId : children) {
            if (out.add(childId)) {
                collectChildIdsRecursively(childId, childrenMap, out);
            }
        }
    }

    /**
     * 转换为RuleVo
     */
    private RuleVo convertToRuleVo(Rule rule, Map<Integer, RuleGroup> groupMap, Map<Integer, RuleProcessor> processorMap) {
        RuleVo vo = new RuleVo();
        BeanUtils.copyProperties(rule, vo);

        vo.setCreatedTimeTxt(DateUtil.formatDateTime(rule.getCreatedTime()));
        vo.setUpdatedTimeTxt(DateUtil.formatDateTime(rule.getUpdatedTime()));
        vo.setStatusTxt(Boolean.TRUE.equals(rule.getStatus()) ? STATUS_PUBLISHED : STATUS_DRAFT);
        vo.setRuleTypeTxt(TYPE_PREFIX + rule.getRuleType());

        Optional.ofNullable(groupMap.get(rule.getGroupId()))
                .ifPresent(group -> vo.setGroupName(buildFullGroupName(group, groupMap)));

        Optional.ofNullable(processorMap.get(rule.getRuleProcessorId()))
                .ifPresent(processor -> vo.setProcessorName(processor.getProcessorName()));

        return vo;
    }

    /**
     * 转换为RuleDetailVo
     */
    private RuleDetailVo convertToDetailVo(RuleVo baseVo) {
        RuleDetailVo detailVo = new RuleDetailVo();
        BeanUtils.copyProperties(baseVo, detailVo);
        return detailVo;
    }

    /**
     * 构建完整分组名称（路径）
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
     * 转换分组为树节点
     */
    private List<Tree> convertGroupsToTreeNodes(List<RuleGroup> groups) {
        return groups.stream().map(group -> {
            Tree node = new Tree();
            node.setId(group.getId());
            node.setName(group.getGroupName());
            node.setType(Constants.TREE_TYPE.GROUP);
            node.setParentId(group.getParentId());
            node.setOrderBy(group.getOrderBy());
            return node;
        }).collect(Collectors.toList());
    }

    /**
     * 转换规则为树节点
     */
    private List<Tree> convertRulesToTreeNodes(List<Rule> rules) {
        return rules.stream().map(rule -> {
            Tree node = new Tree();
            node.setTreeId(rule.getId());
            node.setName(rule.getRuleName());
            node.setType(Constants.TREE_TYPE.RULE);
            node.setParentId(rule.getGroupId());
            node.setOrderBy(0);
            return node;
        }).collect(Collectors.toList());
    }

    /**
     * 构建新规则实体
     */
    private Rule buildNewRule(RuleForm form, RuleGroup group) {
        Rule rule = new Rule();
        rule.setId(CommonUtil.getUUIDString());
        rule.setGroupId(form.getGroupId());
        rule.setRuleCode(RULE_CODE_PREFIX + System.currentTimeMillis());
        rule.setRuleName(form.getRuleName());
        rule.setRuleDescription(form.getRuleDescription());
        rule.setRuleProcessorId(form.getRuleProcessorId());
        rule.setRuleType(resolveRuleType(group));
        rule.setFirstGroupId(resolveFirstGroupId(group));
        rule.setStatus(form.getStatus() != null && form.getStatus());
        rule.setConfig(form.getConfig());
        rule.setDeleted(Constants.DELETE_FLAG.FALSE);

        String currentUser = UserContext.getCurrentAccount();
        Date now = new Date();
        rule.setCreatedBy(currentUser);
        rule.setCreatedTime(now);
        rule.setUpdatedBy(currentUser);
        rule.setUpdatedTime(now);

        return rule;
    }

    /**
     * 构建更新规则实体
     */
    private Rule buildUpdateRule(Rule existingRule, RuleForm form, RuleGroup group) {
        Rule rule = new Rule();
        rule.setId(existingRule.getId());
        rule.setGroupId(form.getGroupId());
        rule.setRuleName(form.getRuleName());
        rule.setRuleCode(StringUtils.defaultIfBlank(form.getRuleCode(), existingRule.getRuleCode()));
        rule.setRuleDescription(form.getRuleDescription());
        rule.setRuleProcessorId(form.getRuleProcessorId());
        rule.setRuleType(resolveRuleType(group));
        rule.setFirstGroupId(resolveFirstGroupId(group));
        rule.setStatus(form.getStatus() != null ? form.getStatus() : existingRule.getStatus());
        rule.setConfig(form.getConfig());
        rule.setUpdatedBy(UserContext.getCurrentAccount());
        rule.setUpdatedTime(new Date());

        return rule;
    }

    /**
     * 构建删除规则实体
     */
    private Rule buildDeleteRule(Rule existingRule) {
        Rule rule = new Rule();
        rule.setId(existingRule.getId());
        rule.setDeleted(Constants.DELETE_FLAG.TRUE);
        rule.setUpdatedBy(UserContext.getCurrentAccount());
        rule.setUpdatedTime(new Date());
        return rule;
    }

    /**
     * 增强规则信息（填充映射数据）
     */
    private void enrichRuleInformation(RuleInformationVo vo, Map<Integer, DBConnectionInfo> dataSourceCache) {
        if (!Objects.equals(vo.getProcessorId(), PROCESSOR_VALUE_MAPPING)) {
            return;
        }

        JSONObject data = extractRuleInformationData(vo.getConfig());
        Integer mappingType = validateAndGetMappingType(data);

        JSONObject mappedData = buildMappedData(data, mappingType, dataSourceCache);
        appendCommonMappingData(mappedData, data, mappingType);

        JSONObject newConfig = new JSONObject();
        newConfig.put("data", mappedData);
        vo.setConfig(newConfig.toString());
    }

    /**
     * 提取规则信息配置数据
     */
    private JSONObject extractRuleInformationData(String configText) {
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
     * 验证并获取映射类型
     */
    private Integer validateAndGetMappingType(JSONObject data) {
        Integer mappingType = data.getInteger("mappingType");
        if (mappingType == null) {
            throw new BusinessException("校验是否选择数据源或者映射方式！");
        }
        return mappingType;
    }

    /**
     * 构建映射数据
     */
    private JSONObject buildMappedData(JSONObject data, Integer mappingType, Map<Integer, DBConnectionInfo> dataSourceCache) {
        JSONObject mappedData = new JSONObject();

        if (mappingType == MAPPING_TYPE_DICT) {
            mappedData.put("fieldList", data.getJSONArray("fieldList"));
        } else if (mappingType == MAPPING_TYPE_CUSTOM_SQL) {
            appendCustomSqlMapping(mappedData, data, dataSourceCache);
        } else {
            appendDatabaseDictMapping(mappedData, data, dataSourceCache);
        }

        return mappedData;
    }

    /**
     * 追加数据库字典表映射
     */
    private void appendDatabaseDictMapping(JSONObject target, JSONObject data, Map<Integer, DBConnectionInfo> dataSourceCache) {
        Integer dataSourceId = data.getInteger("dataSourceId");
        DBConnectionInfo connectionInfo = getConnectionInfoFromCache(dataSourceId, dataSourceCache);
        appendConnectionInfo(target, connectionInfo);

        target.put("tableName", data.getString("tableName"));
        target.put("beforefieldName", data.getString("beforefieldName"));
        target.put("afterfieldName", data.getString("afterfieldName"));
        target.put("condition", data.getJSONObject("condition"));
    }

    /**
     * 追加自定义SQL映射
     */
    private void appendCustomSqlMapping(JSONObject target, JSONObject data, Map<Integer, DBConnectionInfo> dataSourceCache) {
        Integer dataSourceId = data.getInteger("dataSourceId");
        DBConnectionInfo connectionInfo = getConnectionInfoFromCache(dataSourceId, dataSourceCache);
        appendConnectionInfo(target, connectionInfo);

        target.put("sql", data.getString("sqlContext"));
    }

    /**
     * 追加通用映射数据
     */
    private void appendCommonMappingData(JSONObject target, JSONObject source, Integer mappingType) {
        target.put("mappingType", mappingType);
        target.put("rename", source.getString("rename"));
        target.put("renameType", source.getString("renameType"));
        target.put("fieldName", source.getString("fieldName"));
        target.put("fieldType", source.getString("fieldType"));
    }

    /**
     * 执行预览查询
     */
    private Map<String, Object> executePreview(JSONObject configData, DBConnectionInfo connectionInfo, Integer mappingType) {
        String dbType = connectionInfo.getType();
        String sql = buildPreviewSql(configData, mappingType, dbType, connectionInfo);

        try {
            return executeQueryAndBuildResult(sql, connectionInfo, dbType);
        } catch (SQLException e) {
            log.error("[数据预览异常] {}数据库操作异常：{}", dbType, e.getMessage());
            throw new BusinessException("[数据预览异常]:" + dbType + "数据库操作异常：" + e.getMessage());
        }
    }

    /**
     * 构建预览SQL语句
     */
    private String buildPreviewSql(JSONObject configData, Integer mappingType, String dbType, DBConnectionInfo connectionInfo) {
        String sql;

        if (mappingType == MAPPING_TYPE_VISUAL) {
            String beforeFieldName = configData.getString("beforefieldName");
            String afterFieldName = configData.getString("afterfieldName");
            String tableName = configData.getString("tableName");

            tableName = SqlConditionBuilder.buildFullTableName(
                    dbType, connectionInfo.getDbSchema(), connectionInfo.getDbName(), tableName
            );

            String whereClause = buildWhereClause(configData.getJSONObject("condition"), dbType);
            sql = String.format("SELECT %s, %s FROM %s %s", beforeFieldName, afterFieldName, tableName, whereClause);
        } else {
            String customSql = configData.getString("sqlContext");
            SqlConditionBuilder.validateCustomSql(customSql);
            sql = SqlConditionBuilder.cleanPreviewSql(customSql);
        }

        return SqlConditionBuilder.buildPreviewLimitSql(sql, dbType);
    }

    /**
     * 构建WHERE子句
     */
    private String buildWhereClause(JSONObject condition, String dbType) {
        if (condition == null) {
            return "";
        }
        return SqlConditionBuilder.buildWhereClause(condition, dbType);
    }

    /**
     * 执行查询并构建结果
     */
    private Map<String, Object> executeQueryAndBuildResult(String sql, DBConnectionInfo connectionInfo, String dbType) throws SQLException {
        Map<String, Object> result = new HashMap<>();

        try (Connection conn = JdbcUtil.getConnection(
                JdbcUrlUtil.getDriver(dbType),
                JdbcUrlUtil.getUrl(dbType, connectionInfo.getDbHost(), connectionInfo.getDbPort(),
                        connectionInfo.getDbName(), connectionInfo.getProperties()),
                connectionInfo);
             Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery(sql)) {

            SqlConditionBuilder.mapPreviewResult(result, rs, rs.getMetaData());
        }

        return result;
    }

    /**
     * 解析顶层分组ID（根分组）
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
     * 解析规则类型（与顶层分组ID相同）
     */
    private Integer resolveRuleType(RuleGroup group) {
        return resolveFirstGroupId(group);
    }

    /**
     * 验证规则创建权限
     */
    private void validateRuleCreatePermission(RuleGroup selectedGroup) {
        Integer firstGroupId = resolveFirstGroupId(selectedGroup);
        Integer genericId = resolveGenericGroupId();
        Integer publicId = resolvePublicGroupId();
        Integer customId = resolveCustomGroupId();

        // 通用目录下不允许新建
        if (genericId != null && Objects.equals(firstGroupId, genericId)) {
            throw new BusinessException("通用目录下不允许新建规则！");
        }

        boolean isAdmin = userAdminService.validateIsAdmin();

        if (isAdmin) {
            validateAdminPermission(firstGroupId, publicId, customId);
        } else {
            validateNonAdminPermission(firstGroupId, customId);
        }
    }

    /**
     * 验证管理员权限
     */
    private void validateAdminPermission(Integer firstGroupId, Integer publicId, Integer customId) {
        if (publicId != null && customId != null
                && !Objects.equals(firstGroupId, publicId)
                && !Objects.equals(firstGroupId, customId)) {
            throw new BusinessException("管理员仅可在公共/自定义目录下新建规则！");
        }
    }

    /**
     * 验证非管理员权限
     */
    private void validateNonAdminPermission(Integer firstGroupId, Integer customId) {
        if (customId == null || !Objects.equals(firstGroupId, customId)) {
            throw new BusinessException("非管理员仅可在自定义目录及其子级下新建规则！");
        }
    }

    /**
     * 验证规则是否可修改（非通用规则）
     */
    private void validateModifiableRule(Rule rule) {
        if (isGenericRule(rule)) {
            throw new BusinessException("通用规则不允许编辑或删除！");
        }
    }

    /**
     * 判断是否为通用规则
     */
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

    /**
     * 解析通用目录ID
     */
    private Integer resolveGenericGroupId() {
        Integer fromCfg = parseIntOrNull(genericGroupIdCfg);
        return fromCfg != null ? fromCfg : findRootGroupIdByKeyword("通用");
    }

    /**
     * 解析公共目录ID
     */
    private Integer resolvePublicGroupId() {
        Integer fromCfg = parseIntOrNull(publicGroupIdCfg);
        return fromCfg != null ? fromCfg : findRootGroupIdByKeyword("公共");
    }

    /**
     * 解析自定义目录ID
     */
    private Integer resolveCustomGroupId() {
        Integer fromCfg = parseIntOrNull(customGroupIdCfg);
        return fromCfg != null ? fromCfg : findRootGroupIdByKeyword("自定义");
    }

    /**
     * 根据关键词查找根分组ID
     */
    private Integer findRootGroupIdByKeyword(String keyword) {
        LambdaQueryWrapper<RuleGroup> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .isNull(RuleGroup::getParentId);

        List<RuleGroup> roots = groupMapper.selectList(queryWrapper);

        return roots.stream()
                .filter(root -> root.getGroupName() != null && root.getGroupName().contains(keyword))
                .map(RuleGroup::getId)
                .findFirst()
                .orElse(null);
    }

    /**
     * 获取并验证规则
     */
    private Rule getRuleById(String id) {
        if (StringUtils.isBlank(id)) {
            throw new BusinessException("规则ID不能为空");
        }

        Rule rule = ruleMapper.selectById(id);
        if (rule == null || Boolean.TRUE.equals(rule.getDeleted())) {
            throw new BusinessException("规则不存在，请刷新重试！");
        }

        return rule;
    }

    /**
     * 获取并验证分组
     */
    private RuleGroup getAndValidateGroup(Integer groupId) {
        if (groupId == null) {
            throw new BusinessException("分组ID不能为空");
        }

        RuleGroup group = groupMapper.selectById(groupId);
        if (group == null || Boolean.TRUE.equals(group.getDeleted())) {
            throw new BusinessException("请校验该分类是否存在！");
        }

        return group;
    }

    /**
     * 获取活跃的分组列表
     */
    private List<RuleGroup> getActiveGroups() {
        return groupMapper.selectList(new LambdaQueryWrapper<RuleGroup>()
                .eq(RuleGroup::getDeleted, Constants.DELETE_FLAG.FALSE)
                .orderByDesc(RuleGroup::getOrderBy)
                .orderByAsc(RuleGroup::getId));
    }

    /**
     * 获取已发布的规则列表
     */
    private List<Rule> getPublishedRules() {
        return ruleMapper.selectList(new LambdaQueryWrapper<Rule>()
                .eq(Rule::getDeleted, Constants.DELETE_FLAG.FALSE)
                .eq(Rule::getStatus, Boolean.TRUE)
                .orderByDesc(Rule::getUpdatedTime)
                .orderByAsc(Rule::getRuleName));
    }

    /**
     * 获取分组映射
     */
    private Map<Integer, RuleGroup> getGroupMap() {
        return getActiveGroups().stream()
                .collect(Collectors.toMap(RuleGroup::getId, Function.identity(), (a, b) -> a));
    }

    /**
     * 获取处理器映射
     */
    private Map<Integer, RuleProcessor> getProcessorMap() {
        return processorMapper.selectList(new LambdaQueryWrapper<>())
                .stream()
                .collect(Collectors.toMap(RuleProcessor::getId, Function.identity(), (a, b) -> a));
    }

    /**
     * 获取数据库连接信息（带密码解密）
     */
    private DBConnectionInfo getDbConnectionInfo(Integer dataSourceId) {
        validateNotNull(dataSourceId, "数据源");

        DbDatabase database = dbDatabaseService.getDatabaseById(dataSourceId);
        if (database == null) {
            throw new BusinessException("数据源不存在！");
        }

        DBConnectionInfo connectionInfo = new DBConnectionInfo();
        BeanUtils.copyProperties(database, connectionInfo);
        connectionInfo.setPassword(aesUtil.decrypt(database.getPassword()));

        return connectionInfo;
    }

    /**
     * 从缓存获取连接信息
     */
    private DBConnectionInfo getConnectionInfoFromCache(Integer dataSourceId, Map<Integer, DBConnectionInfo> cache) {
        validateNotNull(dataSourceId, "数据源");
        return cache.computeIfAbsent(dataSourceId, dbDatabaseService::getDatabaseConnectionInfo);
    }

    /**
     * 提取并验证Java代码
     */
    private String extractAndValidateJavaCode(JSONObject json) {
        validateProcessorType(json, PROCESSOR_JAVA_CUSTOM, "Java自定义");

        JSONObject configData = extractConfigData(json);
        String javaCode = configData.getString("javaCode");

        validateNotEmpty(javaCode, "Java代码");

        return javaCode;
    }

    /**
     * 提取测试值
     */
    private Object extractTestValue(JSONObject json) {
        JSONObject configData = extractConfigData(json);
        Object value = configData.get("value");

        if (value == null) {
            throw new BusinessException("[Java自定义]：测试值不能为空！");
        }

        return value;
    }

    /**
     * 提取配置数据
     */
    private JSONObject extractConfigData(JSONObject json) {
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
     * 验证处理器类型
     */
    private void validateProcessorType(JSONObject json, int expectedProcessorId, String processorName) {
        Integer processorId = json.getInteger("ruleProcessorId");
        if (!Objects.equals(processorId, expectedProcessorId)) {
            throw new BusinessException(String.format("[%s]：该规则不是%s规则类型！", processorName, processorName));
        }
    }

    /**
     * 验证规则名称唯一性
     */
    private void validateRuleNameUniqueness(String ruleName, Integer groupId, String excludeId) {
        LambdaQueryWrapper<Rule> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(Rule::getRuleName, ruleName)
                .eq(Rule::getGroupId, groupId)
                .eq(Rule::getDeleted, Constants.DELETE_FLAG.FALSE);

        if (StringUtils.isNotBlank(excludeId)) {
            queryWrapper.ne(Rule::getId, excludeId);
        }

        Long count = ruleMapper.selectCount(queryWrapper);
        if (count != null && count > 0) {
            throw new BusinessException("同级分类下规则名称不能重复!");
        }
    }

    /**
     * 验证非空
     */
    private void validateNotEmpty(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new BusinessException(String.format("[值映射]：%s不能为空！", fieldName));
        }
    }

    /**
     * 验证非空
     */
    private void validateNotNull(Object value, String fieldName) {
        if (value == null) {
            throw new BusinessException(String.format("请选择%s！", fieldName));
        }
    }

    /**
     * 解析整数或返回null
     */
    private Integer parseIntOrNull(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 追加连接信息到JSON
     */
    private void appendConnectionInfo(JSONObject target, DBConnectionInfo connectionInfo) {
        if (connectionInfo == null) {
            throw new BusinessException("数据源不存在！");
        }

        target.put("host", connectionInfo.getDbHost());
        target.put("port", connectionInfo.getDbPort());
        target.put("username", connectionInfo.getUsername());
        target.put("password", connectionInfo.getPassword());
        target.put("dbName", connectionInfo.getDbName());
        target.put("dbType", connectionInfo.getType());

        Optional.ofNullable(connectionInfo.getDbSchema()).ifPresent(schema -> target.put("schema", schema));
        Optional.ofNullable(connectionInfo.getProperties()).ifPresent(props -> target.put("properties", props));
    }
}