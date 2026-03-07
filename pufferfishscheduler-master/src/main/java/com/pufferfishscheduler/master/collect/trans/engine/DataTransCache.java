package com.pufferfishscheduler.master.collect.trans.engine;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.trans.TransMeta;

import lombok.extern.slf4j.Slf4j;

/**
 * 数据转换缓存
 */
@Slf4j
public class DataTransCache {
    private final Map<String, TransMeta> cache = new ConcurrentHashMap<>();

    /**
     * 添加或更新缓存
     * @param bizType 业务类型
     * @param bizObjectId 业务对象ID
     * @param transMeta 转换元数据
     */
    public void put(String bizType, String bizObjectId, TransMeta transMeta) {
        if (validateParams(bizType, bizObjectId)) {
            String key = getKey(bizType, bizObjectId);
            cache.put(key, transMeta);
            log.debug("缓存添加/更新成功。键：{}，当前缓存大小：{}", key, cache.size());
        }
    }

    /**
     * 移除缓存
     * @param bizType 业务类型
     * @param bizObjectId 业务对象ID
     */
    public void remove(String bizType, String bizObjectId) {
        if (validateParams(bizType, bizObjectId)) {
            String key = getKey(bizType, bizObjectId);
            cache.remove(key);
            log.debug("缓存移除成功。键：{}，当前缓存大小：{}", key, cache.size());
        }
    }

    /**
     * 获取缓存
     * @param bizType 业务类型
     * @param bizObjectId 业务对象ID
     * @return 转换元数据
     */
    public TransMeta get(String bizType, String bizObjectId) {
        if (validateParams(bizType, bizObjectId)) {
            String key = getKey(bizType, bizObjectId);
            TransMeta transMeta = cache.get(key);
            log.debug("缓存获取。键：{}，是否存在：{}", key, transMeta != null);
            return transMeta;
        }
        return null;
    }

    /**
     * 获取所有缓存值
     * @return 缓存值集合
     */
    public Collection<TransMeta> getAll() {
        return cache.values();
    }

    /**
     * 获取缓存大小
     * @return 缓存大小
     */
    public int size() {
        return cache.size();
    }

    /**
     * 清空缓存
     */
    public void clear() {
        int size = cache.size();
        cache.clear();
        log.debug("缓存已清空。清空前大小：{}", size);
    }

    /**
     * 检查缓存是否包含指定键
     * @param bizType 业务类型
     * @param bizObjectId 业务对象ID
     * @return 是否包含
     */
    public boolean containsKey(String bizType, String bizObjectId) {
        if (validateParams(bizType, bizObjectId)) {
            String key = getKey(bizType, bizObjectId);
            return cache.containsKey(key);
        }
        return false;
    }

    /**
     * 生成缓存键
     * @param bizType 业务类型
     * @param bizObjectId 业务对象ID
     * @return 缓存键
     */
    private String getKey(String bizType, String bizObjectId) {
        return String.format("%s-%s", bizType, bizObjectId);
    }

    /**
     * 验证参数
     * @param bizType 业务类型
     * @param bizObjectId 业务对象ID
     * @return 参数是否有效
     */
    private boolean validateParams(String bizType, String bizObjectId) {
        if (StringUtils.isBlank(bizType)) {
            log.warn("业务类型不能为空");
            return false;
        }
        if (StringUtils.isBlank(bizObjectId)) {
            log.warn("业务对象ID不能为空");
            return false;
        }
        return true;
    }
}
