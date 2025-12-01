package com.pufferfishscheduler.service.dict.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.service.database.DbGroupService;
import com.pufferfishscheduler.service.dict.service.DictService;
import com.pufferfishscheduler.domain.vo.dict.Dict;
import com.pufferfishscheduler.domain.vo.dict.DictItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 字典 ServiceImpl
 */
@Slf4j
@Service
public class DictServiceImpl implements DictService {

    // 使用分层缓存结构：第一层是字典code，第二层是字典项code到值的映射
    private final Map<String, Map<String, String>> dictCache = new ConcurrentHashMap<>();
    private final Map<String, List<DictItem>> dictItemCache = new ConcurrentHashMap<>();

    // 添加反向映射缓存，提高根据value查找code的性能
    private final Map<String, Map<String, String>> reverseDictCache = new ConcurrentHashMap<>();

    private final DbGroupService dbGroupService;
    private final ObjectMapper objectMapper;

    @Autowired
    public DictServiceImpl(DbGroupService dbGroupService, ObjectMapper objectMapper) {
        this.dbGroupService = dbGroupService;
        this.objectMapper = objectMapper;
    }

    /**
     * 初始化字典数据
     */
    @PostConstruct
    private void init() {
        loadDictData();
    }

    /**
     * 每分钟刷新字典
     */
    @Scheduled(cron = "0 * * * * *")
    private void refresh() {
        log.info("开始重新加载字典数据...");
        loadDictData();
        log.info("字典数据重新加载完成，当前缓存字典数量: {}", dictCache.size());
    }

    /**
     * 加载字典数据
     */
    private synchronized void loadDictData() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(Constants.DICT_FILE)) {
            if (inputStream == null) {
                log.error("字典文件未找到: {}", Constants.DICT_FILE);
                return;
            }

            List<Dict> dictList = objectMapper.readValue(inputStream, new TypeReference<List<Dict>>() {});

            // 创建临时缓存，避免在加载过程中影响现有缓存
            Map<String, Map<String, String>> tempDictCache = new ConcurrentHashMap<>();
            Map<String, List<DictItem>> tempDictItemCache = new ConcurrentHashMap<>();
            Map<String, Map<String, String>> tempReverseCache = new ConcurrentHashMap<>();

            // 加载静态字典
            for (Dict dict : dictList) {
                String dictCode = dict.getDictCode();
                List<DictItem> items = Optional.ofNullable(dict.getList())
                        .orElse(Collections.emptyList());

                // 缓存字典项列表
                tempDictItemCache.put(dictCode, Collections.unmodifiableList(items));
                loadDictItemMapping(dictCode, items, tempDictCache, tempReverseCache);
            }

            // 加载业务字典
            loadBusinessDict(tempDictCache, tempDictItemCache, tempReverseCache);

            // 原子性更新缓存
            dictCache.clear();
            dictItemCache.clear();
            reverseDictCache.clear();

            dictCache.putAll(tempDictCache);
            dictItemCache.putAll(tempDictItemCache);
            reverseDictCache.putAll(tempReverseCache);

            log.info("成功加载 {} 个字典配置", dictCache.size());

        } catch (IOException e) {
            log.error("加载字典数据失败: {}", e.getMessage(), e);
            throw new IllegalStateException("字典数据初始化失败", e);
        }
    }

    /**
     * 加载业务字典
     */
    private void loadBusinessDict(Map<String, Map<String, String>> dictCache,
                                  Map<String, List<DictItem>> dictItemCache,
                                  Map<String, Map<String, String>> reverseCache) {
        try {
            List<DictItem> itemList = dbGroupService.dict();
            if (!CollectionUtils.isEmpty(itemList)) {
                String dictCode = Constants.DICT.DB_GROUP;
                dictItemCache.put(dictCode, Collections.unmodifiableList(itemList));
                loadDictItemMapping(dictCode, itemList, dictCache, reverseCache);
            }
        } catch (Exception e) {
            log.error("加载业务字典失败: {}", e.getMessage(), e);
            // 业务字典加载失败不应影响整个字典服务的启动
        }
    }

    /**
     * 加载字典映射
     */
    private void loadDictItemMapping(String dictCode, List<DictItem> itemList,
                                     Map<String, Map<String, String>> dictCache,
                                     Map<String, Map<String, String>> reverseCache) {
        if (CollectionUtils.isEmpty(itemList)) {
            dictCache.put(dictCode, Collections.emptyMap());
            reverseCache.put(dictCode, Collections.emptyMap());
            return;
        }

        // 构建正向映射 (code -> value)
        Map<String, String> itemMap = itemList.stream()
                .filter(item -> item.getCode() != null)
                .collect(Collectors.toMap(
                        DictItem::getCode,
                        DictItem::getValue,
                        (existing, replacement) -> {
                            log.warn("字典 {} 中存在重复的字典项编码: {}, 使用前者: {}",
                                    dictCode, replacement, existing);
                            return existing;
                        }
                ));

        // 构建反向映射 (value -> code)
        Map<String, String> reverseMap = itemList.stream()
                .filter(item -> item.getCode() != null && item.getValue() != null)
                .collect(Collectors.toMap(
                        DictItem::getValue,
                        DictItem::getCode,
                        (existing, replacement) -> {
                            log.warn("字典 {} 中存在重复的字典项值: {}, 使用前者: {}",
                                    dictCode, replacement, existing);
                            return existing;
                        }
                ));

        dictCache.put(dictCode, Collections.unmodifiableMap(itemMap));
        reverseCache.put(dictCode, Collections.unmodifiableMap(reverseMap));
    }

    /**
     * 获取字典项列表
     */
    @Override
    public List<DictItem> getDict(String dictCode) {
        if (dictCode == null) {
            return Collections.emptyList();
        }
        return dictItemCache.getOrDefault(dictCode, Collections.emptyList());
    }

    /**
     * 根据字典code与字典项code获取字典项value
     */
    @Override
    public String getDictItemValue(String dictCode, String itemCode) {
        if (dictCode == null || itemCode == null) {
            return "";
        }

        Map<String, String> itemMap = dictCache.get(dictCode);
        if (itemMap == null) {
            log.debug("字典不存在: {}", dictCode);
            return "";
        }

        String value = itemMap.get(itemCode);
        return value != null ? value : "";
    }

    /**
     * 根据字典code与字典项value获取字典项code
     */
    @Override
    public String getDictItemCode(String dictCode, String itemValue) {
        if (dictCode == null || itemValue == null) {
            return "";
        }

        Map<String, String> reverseMap = reverseDictCache.get(dictCode);
        if (reverseMap == null) {
            log.debug("字典不存在: {}", dictCode);
            return "";
        }

        String code = reverseMap.get(itemValue);
        return code != null ? code : "";
    }
}