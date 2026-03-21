package com.pufferfishscheduler.master.collect.trans.engine;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.pufferfishscheduler.common.utils.CommonUtil;
import org.w3c.dom.Document;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

import com.pufferfishscheduler.common.enums.FlowType;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.dao.entity.KettleFlowRepository;
import com.pufferfishscheduler.dao.mapper.KettleFlowRepositoryMapper;
import com.pufferfishscheduler.master.collect.trans.engine.entity.TransFlowConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * 数据流程配置仓库
 */
@Slf4j
@Component
public class DataFlowRepository {

    private static volatile DataFlowRepository repository;
    private static KettleFlowRepositoryMapper kettleFlowRepositoryMapper;

    /**
     * 构造方法，通过依赖注入获取KettleFlowRepositoryMapper
     */
    @Autowired
    public DataFlowRepository(KettleFlowRepositoryMapper mapper) {
        kettleFlowRepositoryMapper = mapper;
        repository = this;
        log.info("DataFlowRepository initialized with KettleFlowRepositoryMapper");
    }

    /**
     * 初始化仓库实例
     * 
     * @return 仓库实例
     */
    public static DataFlowRepository init() {
        if (repository == null) {
            synchronized (DataFlowRepository.class) {
                if (repository == null) {
                    log.warn("DataFlowRepository is not initialized yet. It should be managed by Spring container.");
                }
            }
        }
        return repository;
    }

    /**
     * 获取仓库实例
     * 
     * @return 仓库实例
     */
    public static DataFlowRepository getRepository() {
        if (repository == null) {
            throw new IllegalStateException("DataFlowRepository has not been initialized. Call init() first.");
        }
        return repository;
    }

    /**
     * 保存转换流程配置
     * 
     * @param transFlowConfig 转换流程配置
     */
    public void saveTrans(TransFlowConfig transFlowConfig) {
        save(transFlowConfig);
    }

    /**
     * 更新转换流程配置
     * 
     * @param transFlowConfig 转换流程配置
     */
    public void updateTrans(TransFlowConfig transFlowConfig) {
        update(transFlowConfig);
    }

    /**
     * 删除转换流程配置
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     */
    public void deleteTrans(String bizType, String bizObjectId) {
        delete(bizType, bizObjectId);
    }

    /**
     * 根据业务表关联ID查询转换流程配置
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     * @return 转换流程配置
     */
    public TransFlowConfig getTrans(String bizType, String bizObjectId) {
        validateBizParams(bizType, bizObjectId);
        
        KettleFlowRepository kettleFlowRepository = kettleFlowRepositoryMapper.selectByBizTypeAndBizObjectId(bizType, bizObjectId);
        if (kettleFlowRepository != null && FlowType.Trans.name().equals(kettleFlowRepository.getFlowType())) {
            return convertToTransFlowConfig(kettleFlowRepository);
        }
        return null;
    }

    /**
     * 更新转换流程配置的主机名
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     * @param hostName    主机名
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void updateHostName(String bizType, String bizObjectId, String hostName) {
        validateBizParams(bizType, bizObjectId);
        if (StringUtils.isBlank(hostName)) {
            throw new IllegalArgumentException("Host name cannot be blank");
        }
        
        kettleFlowRepositoryMapper.updateHostName(hostName, bizType, bizObjectId);
    }

    /**
     * 根据主机名查询所有转换流程配置
     * 
     * @param hostName 主机名
     * @return 转换流程配置列表
     */
    public List<TransFlowConfig> getAllFlowsByHostName(String hostName) {
        if (StringUtils.isBlank(hostName)) {
            throw new IllegalArgumentException("Host name cannot be blank");
        }
        
        List<KettleFlowRepository> kettleFlowRepositories = kettleFlowRepositoryMapper.selectByHostName(hostName);
        if (CollectionUtils.isEmpty(kettleFlowRepositories)) {
            return new ArrayList<>();
        }
        
        List<TransFlowConfig> transFlowConfigs = new ArrayList<>();
        for (KettleFlowRepository kettleFlowRepository : kettleFlowRepositories) {
            transFlowConfigs.add(convertToTransFlowConfig(kettleFlowRepository));
        }
        return transFlowConfigs;
    }

    /**
     * 将XML字符串转换为转换流程元数据
     * 
     * @param xml XML字符串
     * @return 转换流程元数据
     */
    public static TransMeta xml2TransMeta(String xml) {
        if (StringUtils.isBlank(xml)) {
            throw new IllegalArgumentException("XML string cannot be blank");
        }
        
        try {
            Document doc = XMLHandler.loadXMLString(xml);
            TransMeta transMeta = new TransMeta(XMLHandler.getSubNode(doc, "transformation"), (Repository) null);
            return transMeta;
        } catch (Exception e) {
            log.error("Failed to convert XML to TransMeta", e);
            throw new RuntimeException("Failed to convert XML to TransMeta", e);
        }
    }

    /**
     * 保存转换流程配置
     * 
     * @param transFlowConfig 转换流程配置
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    protected void save(TransFlowConfig transFlowConfig) {
        validateTransFlowConfig(transFlowConfig);
        
        String currentUser = UserContext.getCurrentAccount();
        transFlowConfig.setCreatedBy(currentUser);
        transFlowConfig.setCreatedTime(new Date());
        transFlowConfig.setUpdatedBy(currentUser);
        transFlowConfig.setUpdatedTime(new Date());
        transFlowConfig.setVersion(1);
        transFlowConfig.setFlowStatus(1);
        transFlowConfig.setId(CommonUtil.getUUIDString());
        // 使用自定义 XML 的 insertFlow，避免 BaseMapper 插入规则差异
        int insert = kettleFlowRepositoryMapper.insertFlow(transFlowConfig);
        log.info("Saved {} rows for trans flow config: {}-{}", insert, transFlowConfig.getBizType(), transFlowConfig.getBizObjectId());
    }

    /**
     * 更新转换流程配置
     * 
     * @param transFlowConfig 转换流程配置
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    protected void update(TransFlowConfig transFlowConfig) {
        validateTransFlowConfig(transFlowConfig);
        
        String currentUser = UserContext.getCurrentAccount();
        int update = kettleFlowRepositoryMapper.updateFlow(
                transFlowConfig.getFlowType(),
                transFlowConfig.getFlowContent(),
                transFlowConfig.getBizType(),
                transFlowConfig.getBizObjectId()
        );
        log.info("Updated {} rows for trans flow config: {}-{}", update, transFlowConfig.getBizType(), transFlowConfig.getBizObjectId());
    }

    /**
     * 删除转换流程配置
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    protected void delete(String bizType, String bizObjectId) {
        validateBizParams(bizType, bizObjectId);
        
        kettleFlowRepositoryMapper.deleteFlow(bizType, bizObjectId);
        log.info("Deleted trans flow config: {}-{}", bizType, bizObjectId);
    }

    /**
     * 根据业务对象ID查询转换流程配置
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     * @return 转换流程配置
     */
    private TransFlowConfig get(String bizType, String bizObjectId) {
        validateBizParams(bizType, bizObjectId);
        
        KettleFlowRepository kettleFlowRepository = kettleFlowRepositoryMapper.selectByBizTypeAndBizObjectId(bizType, bizObjectId);
        if (kettleFlowRepository == null) {
            String errorMsg = String.format("Kettle flow repository not found for bizType: %s, bizObjectId: %s", bizType, bizObjectId);
            log.error(errorMsg);
            throw new BusinessException(errorMsg);
        }
        return convertToTransFlowConfig(kettleFlowRepository);
    }

    /**
     * 验证业务参数
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     */
    private void validateBizParams(String bizType, String bizObjectId) {
        if (StringUtils.isBlank(bizType)) {
            throw new IllegalArgumentException("Business type cannot be blank");
        }
        if (StringUtils.isBlank(bizObjectId)) {
            throw new IllegalArgumentException("Business object ID cannot be blank");
        }
    }

    /**
     * 验证转换流程配置
     * 
     * @param transFlowConfig 转换流程配置
     */
    private void validateTransFlowConfig(TransFlowConfig transFlowConfig) {
        if (transFlowConfig == null) {
            throw new IllegalArgumentException("Trans flow config cannot be null");
        }
        validateBizParams(transFlowConfig.getBizType(), transFlowConfig.getBizObjectId());
        if (StringUtils.isBlank(transFlowConfig.getFlowType())) {
            throw new IllegalArgumentException("Flow type cannot be blank");
        }
        if (StringUtils.isBlank(transFlowConfig.getFlowContent())) {
            throw new IllegalArgumentException("Flow content cannot be blank");
        }
    }

    /**
     * 将KettleFlowRepository转换为TransFlowConfig
     * 
     * @param kettleFlowRepository KettleFlowRepository对象
     * @return TransFlowConfig对象
     */
    private TransFlowConfig convertToTransFlowConfig(KettleFlowRepository kettleFlowRepository) {
        TransFlowConfig transFlowConfig = new TransFlowConfig();
        transFlowConfig.setFlowType(kettleFlowRepository.getFlowType());
        transFlowConfig.setBizType(kettleFlowRepository.getBizType());
        transFlowConfig.setBizObjectId(kettleFlowRepository.getBizObjectId());
        transFlowConfig.setFlowContent(kettleFlowRepository.getFlowContent());
        transFlowConfig.setExecutorHost(kettleFlowRepository.getExecutorHost());
        transFlowConfig.setId(kettleFlowRepository.getId());
        transFlowConfig.setUpdatedTime(kettleFlowRepository.getUpdatedTime());
        return transFlowConfig;
    }
}
