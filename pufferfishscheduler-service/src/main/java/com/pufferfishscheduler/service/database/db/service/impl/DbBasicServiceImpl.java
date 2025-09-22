package com.pufferfishscheduler.service.database.db.service.impl;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.domain.vo.database.DbBasicVo;
import com.pufferfishscheduler.domain.vo.database.DbCategoryVo;
import com.pufferfishscheduler.dao.entity.DbCategory;
import com.pufferfishscheduler.dao.mapper.DbBasicMapper;
import com.pufferfishscheduler.dao.mapper.DbCategoryMapper;
import com.pufferfishscheduler.service.database.db.service.DbBasicService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * (DbBasic)表服务实现类
 *
 * @author mayc
 * @since 2025-05-20 23:34:43
 */
@Slf4j
@Service("dbBasicService")
public class DbBasicServiceImpl implements DbBasicService {

    @Autowired
    private DbCategoryMapper dbCategoryDao;

    @Autowired
    private DbBasicMapper dbBasicDao;

    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * 缓存数据源图标
     */
    public static final Map<String, String> CACHE_ICON = new ConcurrentHashMap<>();

    @PostConstruct
    private void init() {

        try {
            redisTemplate.opsForValue().set(
                    Constants.REDIS_KEY.CATEGORY,
                    convertToCategoryVoList(dbCategoryDao.selectList(null)),
                    1, TimeUnit.DAYS
            );

            redisTemplate.opsForValue().set(
                    Constants.REDIS_KEY.DB_BASIC,
                    dbBasicDao.selectDbBasicList(),
                    1, TimeUnit.DAYS
            );

            dbBasicDao.selectDbBasicList().forEach(basic ->
                    CACHE_ICON.put(basic.getName(), basic.getImg())
            );
        } catch (Exception e) {
            log.error("数据库基础信息初始化失败", e);
        }
    }

    /**
     * 转换特殊分类
     *
     * @param categories
     * @return
     */
    private List<DbCategoryVo> convertToCategoryVoList(List<DbCategory> categories) {
        return categories.stream().map(category -> {
            DbCategoryVo vo = new DbCategoryVo();
            vo.setId(category.getId());
            vo.setImg(category.getImg());
            vo.setImgConfig(category.getImgConfig());
            vo.setName(category.getName());
            vo.setOrderBy(category.getOrderBy());
            vo.setCreatedBy(category.getCreatedBy());
            vo.setCreatedTime(category.getCreatedTime());
            return vo;
        }).collect(Collectors.toList());
    }

    /**
     * 从缓存中获取集合
     *
     * @param key
     * @param loader
     * @param <T>
     * @return
     */
    private <T> List<T> getCachedList(String key, Supplier<List<T>> loader) {
        // 第一次尝试
        try {
            List<T> cache = (List<T>) redisTemplate.opsForValue().get(key);
            if (cache != null) {
                log.debug("Cache hit for key: {}", key);
                return cache;
            }
        } catch (Exception e) {
            log.warn("First cache read error for key: {}", key, e);
        }

        // 加载数据
        log.debug("Cache miss for key: {}", key);
        List<T> data = loader.get();

        if (!data.isEmpty()) {
            // 重试机制
            int retry = 0;
            while (retry < 3) {
                try {
                    redisTemplate.opsForValue().set(key, data, 1, TimeUnit.DAYS);
                    break;
                } catch (Exception e) {
                    retry++;
                    log.warn("Cache write error (attempt {}): {}", retry, e.getMessage());
                    if (retry == 3) {
                        log.error("Failed to write cache after 3 attempts", e);
                    }
                    try {
                        Thread.sleep(500L * retry);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return data;
    }

    /**
     * 获取数据库特殊分类集合
     *
     * @return
     */
    @Override
    public List<DbCategoryVo> getDbCategoryList() {
        return getCachedList(Constants.REDIS_KEY.CATEGORY,
                () -> convertToCategoryVoList(dbCategoryDao.selectList(null)));
    }

    /**
     * 获取数据库基础信息
     *
     * @return
     */
    @Override
    public List<DbBasicVo> getDbBasicList() {
        return getCachedList(Constants.REDIS_KEY.DB_BASIC,
                dbBasicDao::selectDbBasicList);
    }


    /**
     * query database basic list by category id
     *
     * @param categoryId
     * @return
     */
    @Override
    public List<DbBasicVo> getDbBasicListByCategoryId(Integer categoryId) {
        return dbBasicDao.getDbBasicListByCategoryId(categoryId);
    }

    /**
     * 获取数据源图标
     *
     * @param name
     * @return
     */
    @Override
    public String getDbIcon(String name) {
        return CACHE_ICON.getOrDefault(name, "");
    }

}

