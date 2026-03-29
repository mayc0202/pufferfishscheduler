package com.pufferfishscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.pufferfishscheduler.dao.entity.Rule;
import com.pufferfishscheduler.domain.vo.collect.RuleInformationVo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 规则Mapper
 */
@Mapper
@Repository
public interface RuleMapper extends BaseMapper<Rule> {

    /**
     * 获取规则信息(已发布)
     */
    List<RuleInformationVo> getRuleInformation(@Param(value = "ruleIds") List<String> ruleIds);
}