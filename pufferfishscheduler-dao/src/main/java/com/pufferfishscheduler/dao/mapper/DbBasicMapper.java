package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.DbBasic;
import com.pufferfishscheduler.domain.vo.database.DbBasicVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * (DbBasic)表数据库访问层
 *
 * @author mayc
 * @since 2025-05-20 23:34:42
 */
@Mapper
public interface DbBasicMapper extends BaseMapper<DbBasic> {

    /**
     * query database basic list
     *
     * @return
     */
    List<DbBasicVo> selectDbBasicList();

    /**
     * query database basic list by category id
     *
     * @param categoryId
     * @return
     */
    List<DbBasicVo> getDbBasicListByCategoryId(@Param("categoryId") Integer categoryId);
}

