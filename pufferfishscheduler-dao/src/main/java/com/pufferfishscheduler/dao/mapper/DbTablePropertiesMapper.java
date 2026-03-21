package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.DbTableProperties;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * 数据库表属性Mapper接口
 *
 * @author Mayc
 * @since 2026-03-17  18:22
 */
@Mapper
public interface DbTablePropertiesMapper extends BaseMapper<DbTableProperties> {

    /**
     * 批量插入表属性配置
     */
    void batchInsert(List<DbTableProperties> list);
}
