package com.pufferfishscheduler.worker.task.trans.engine;

import com.pufferfishscheduler.common.enums.FlowType;
import com.pufferfishscheduler.worker.task.trans.engine.entity.TransFlowConfig;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.trans.TransMeta;

import java.util.List;

/**
 * 数据缓存
 */
@Slf4j
public class DataCache {
    private static volatile boolean enable = false;
    private static volatile DataTransCache transCache;
    private static final Object LOCK = new Object();

    private DataCache() {
    }

    /**
     * 初始化缓存
     */
    public static void init() {
        synchronized (LOCK) {
            if (transCache == null) {
                transCache = new DataTransCache();
                log.info("Data cache initialized");
            }
        }
    }

    /**
     * 重新构建缓存
     * 
     * @param rep      数据流程仓库
     * @param hostName 主机名
     */
    public static void reBuildCache(DataFlowRepository rep, String hostName) {
        if (rep == null) {
            log.error("DataFlowRepository cannot be null");
            return;
        }
        if (hostName == null || hostName.isEmpty()) {
            log.error("Host name cannot be null or empty");
            return;
        }

        synchronized (LOCK) {
            log.info("Rebuilding cache for host: {}", hostName);
            DataTransCache transCacheTemp = new DataTransCache();
            try {
                List<TransFlowConfig> list = rep.getAllFlowsByHostName(hostName);
                if (list != null && !list.isEmpty()) {
                    int count = 0;
                    for (TransFlowConfig efc : list) {
                        if (FlowType.Trans.name().equals(efc.getFlowType())) {
                            try {
                                TransMeta transMeta = DataFlowRepository.xml2TransMeta(efc.getFlowContent());
                                transCacheTemp.put(efc.getBizType(), efc.getBizObjectId(), transMeta);
                                count++;
                            } catch (Exception e) {
                                log.error("Failed to parse trans meta for bizType: {}, bizObjectId: {}", 
                                        efc.getBizType(), efc.getBizObjectId(), e);
                            }
                        }
                    }
                    transCache = transCacheTemp;
                    log.info("Cache rebuilt successfully, loaded {} trans flows", count);
                } else {
                    transCache = transCacheTemp;
                    log.warn("No flows found in repository for host: {}", hostName);
                }
            } catch (Exception e) {
                log.error("Failed to rebuild cache", e);
            }
        }
    }

    /**
     * 获取转换元数据
     * 
     * @param bizType     业务类型
     * @param bizObjectId 业务对象ID
     * @return 转换元数据
     */
    public static TransMeta getTransMeta(String bizType, String bizObjectId) {
        if (!enable || transCache == null) {
            return null;
        }
        return transCache.get(bizType, bizObjectId);
    }

    /**
     * 检查缓存是否启用
     * 
     * @return 是否启用
     */
    public static boolean isEnable() {
        return enable;
    }

    /**
     * 设置缓存启用状态
     * 
     * @param enable 是否启用
     */
    public static void setEnable(boolean enable) {
        DataCache.enable = enable;
        log.info("Cache enabled: {}", enable);
    }

    /**
     * 清理缓存
     */
    public static void clear() {
        synchronized (LOCK) {
            if (transCache != null) {
                transCache.clear();
                log.info("Cache cleared");
            }
        }
    }

    /**
     * 获取缓存大小
     * 
     * @return 缓存大小
     */
    public static int getSize() {
        if (transCache == null) {
            return 0;
        }
        return transCache.size();
    }
}