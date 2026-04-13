package com.pufferfishscheduler.worker.task.trans.engine;

import com.pufferfishscheduler.common.bean.UserContext;
import com.pufferfishscheduler.common.enums.FlowType;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.utils.CommonUtil;
import com.pufferfishscheduler.dao.entity.KettleFlowRepository;
import com.pufferfishscheduler.dao.mapper.KettleFlowRepositoryMapper;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransFlowConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
            Node transRoot = XMLHandler.getSubNode(doc, "transformation");
            if (transRoot == null && doc.getDocumentElement() != null
                    && "transformation".equalsIgnoreCase(doc.getDocumentElement().getNodeName())) {
                transRoot = doc.getDocumentElement();
            }
            if (transRoot == null) {
                throw new IllegalArgumentException("XML does not contain a <transformation> root element");
            }
            patchBlankStepNames(transRoot);
            patchEmptyKafkaConsumerStepType(transRoot);
            patchEmptyRabbitMqConsumerStepType(transRoot);
            XMLHandlerCache.getInstance().clear();
            TransMeta transMeta = new TransMeta(transRoot, (Repository) null);
            return transMeta;
        } catch (Exception e) {
            log.error("Failed to convert XML to TransMeta", e);
            throw new RuntimeException("Failed to convert XML to TransMeta", e);
        }
    }

    /**
     * 为缺少 {@code <name>} 或文本为空的 {@code <step>} 补占位名，避免 Kettle 解析 TransLogTable 时在
     * {@link org.pentaho.di.trans.step.StepMeta#findStep} 中对 null 名称调用 {@code equalsIgnoreCase} 崩溃。
     */
    private static void patchBlankStepNames(Node transRoot) {
        if (transRoot == null) {
            return;
        }
        int nr = XMLHandler.countNodes(transRoot, "step");
        for (int i = 0; i < nr; i++) {
            Node stepNode = XMLHandler.getSubNodeByNr(transRoot, "step", i, false);
            if (stepNode == null) {
                continue;
            }
            String stepName = XMLHandler.getTagValue(stepNode, "name");
            if (StringUtils.isNotBlank(stepName)) {
                continue;
            }
            String fallback = "Unnamed_step_" + (i + 1);
            Node nameNode = XMLHandler.getSubNode(stepNode, "name");
            if (nameNode != null) {
                clearChildText(nameNode);
                nameNode.appendChild(nameNode.getOwnerDocument().createTextNode(fallback));
            } else {
                Document doc = stepNode.getOwnerDocument();
                Element nameEl = doc.createElement("name");
                nameEl.appendChild(doc.createTextNode(fallback));
                stepNode.appendChild(nameEl);
            }
            log.warn("已修补空步骤名为: {}（原 name 缺失或为空）", fallback);
        }
    }

    private static void clearChildText(Node parent) {
        NodeList children = parent.getChildNodes();
        for (int j = children.getLength() - 1; j >= 0; j--) {
            parent.removeChild(children.item(j));
        }
    }

    /**
     * 历史/前端落库的 KTR 曾出现 {@code <type/>} 为空，Kettle 会报 plugin missing。
     * 根据本工程 KafkaConsumerInputMeta 的 XML 形态补全为 {@code KafkaConsumerInput}。
     */
    private static void patchEmptyKafkaConsumerStepType(Node transRoot) {
        if (transRoot == null) {
            return;
        }
        int nr = XMLHandler.countNodes(transRoot, "step");
        for (int i = 0; i < nr; i++) {
            Node stepNode = XMLHandler.getSubNodeByNr(transRoot, "step", i, false);
            if (stepNode == null) {
                continue;
            }
            String type = XMLHandler.getTagValue(stepNode, "type");
            if (StringUtils.isNotBlank(type)) {
                continue;
            }
            if (!isKafkaConsumerInputStepXml(stepNode)) {
                continue;
            }
            String stepName = XMLHandler.getTagValue(stepNode, "name");
            Node typeNode = XMLHandler.getSubNode(stepNode, "type");
            if (typeNode != null) {
                typeNode.setTextContent("KafkaConsumerInput");
            } else {
                Document doc = stepNode.getOwnerDocument();
                Element typeEl = doc.createElement("type");
                typeEl.setTextContent("KafkaConsumerInput");
                stepNode.appendChild(typeEl);
            }
            log.warn("已修补空的步骤 <type> 为 KafkaConsumerInput（步骤名: {}）", stepName);
        }
    }

    private static boolean isKafkaConsumerInputStepXml(Node stepNode) {
        if (XMLHandler.getSubNode(stepNode, "directBootstrapServers") != null) {
            return true;
        }
        String conn = XMLHandler.getTagValue(stepNode, "connectionType");
        return StringUtils.isNotBlank(conn)
                && XMLHandler.getSubNode(stepNode, "consumerGroup") != null
                && XMLHandler.countNodes(stepNode, "topic") > 0;
    }

    /**
     * 补全 RabbitMQ 输入步骤空 {@code <type/>}。
     */
    private static void patchEmptyRabbitMqConsumerStepType(Node transRoot) {
        if (transRoot == null) {
            return;
        }
        int nr = XMLHandler.countNodes(transRoot, "step");
        for (int i = 0; i < nr; i++) {
            Node stepNode = XMLHandler.getSubNodeByNr(transRoot, "step", i, false);
            if (stepNode == null) {
                continue;
            }
            String type = XMLHandler.getTagValue(stepNode, "type");
            if (StringUtils.isNotBlank(type)) {
                continue;
            }
            if (!isRabbitMqConsumerInputStepXml(stepNode)) {
                continue;
            }
            String stepName = XMLHandler.getTagValue(stepNode, "name");
            Node typeNode = XMLHandler.getSubNode(stepNode, "type");
            if (typeNode != null) {
                typeNode.setTextContent("RabbitMqConsumerInput");
            } else {
                Document doc = stepNode.getOwnerDocument();
                Element typeEl = doc.createElement("type");
                typeEl.setTextContent("RabbitMqConsumerInput");
                stepNode.appendChild(typeEl);
            }
            log.warn("已修补空的步骤 <type> 为 RabbitMqConsumerInput（步骤名: {}）", stepName);
        }
    }

    private static boolean isRabbitMqConsumerInputStepXml(Node stepNode) {
        if (XMLHandler.getSubNode(stepNode, "directBootstrapServers") != null) {
            return false;
        }
        return XMLHandler.getSubNode(stepNode, "queue") != null
                && XMLHandler.getSubNode(stepNode, "virtualHost") != null
                && XMLHandler.getSubNode(stepNode, "host") != null;
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
                transFlowConfig.getFlowJson(),
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
        transFlowConfig.setFlowJson(kettleFlowRepository.getFlowJson());
        transFlowConfig.setExecutorHost(kettleFlowRepository.getExecutorHost());
        transFlowConfig.setId(kettleFlowRepository.getId());
        transFlowConfig.setUpdatedTime(kettleFlowRepository.getUpdatedTime());
        return transFlowConfig;
    }
}
