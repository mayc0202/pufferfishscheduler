package com.pufferfishscheduler.master.collect.realtime.engine.kafka.entity;

import java.util.ArrayList;
import java.util.List;

import com.pufferfishscheduler.common.exception.BusinessException;

import com.pufferfishscheduler.dao.entity.RtTableMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 表映射
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableMapper {

    /**
     * 表映射ID
     */
    private Integer tableMapperId;

    /**
     * 来源表
     */
    private String sourceTableName;

    /**
     * 目标表
     */
    private String targetTableName;

    /**
     * 写入类型
     */
    private String writeType;

    /**
     * 批次大小
     */
    private Integer batchSize;

    /**
     * 任务首次启动后是否删除数据。1 - 任务首次启动删除目标库中目标表的数据； 0 - 不删除目标库目标表数据。默认：否
     */
    private Boolean deleteDataFlag;

    /**
     * 是否并行写入。1 - 并行写入； 0 - 串行写入。默认：否
     */
    private Boolean parallelWriteFlag;

    /**
     * 并发写入线程数量。只有当parallel_write_flag = 1的时候，此值才会必填。默认值是5，最大值10
     */
    private Integer parallelThreadNum;

    /**
     * 字段映射
     */
    private List<FieldMapper> fieldMappers;

    /**
     * 是否开启心跳。1 - 开启； 0 - 不开启。默认：否
     */
    private Boolean heartbeatEnabled;

    /**
     * 心跳间隔（单位：秒）。默认值是60秒
     */
    private Integer heartbeatInterval;

    public TableMapper(RtTableMapper rtTableMapper) {
        this(rtTableMapper, null);
    }

    public TableMapper(RtTableMapper rtTableMapper,List<FieldMapper> fieldMappers) {
        this.deleteDataFlag = rtTableMapper.getDeleteDataFlag();
        this.batchSize = rtTableMapper.getBatchSize();
        this.writeType = rtTableMapper.getWriteType();
        this.parallelWriteFlag = rtTableMapper.getParallelWriteFlag();
        this.parallelThreadNum = rtTableMapper.getParallelThreadNum();

        this.sourceTableName = rtTableMapper.getSourceTableName();
        this.targetTableName = rtTableMapper.getTargetTableName();
        this.fieldMappers = fieldMappers;
    }

    /**
     * 验证表映射是否合法
     */
    public void valid() {
        if (tableMapperId == null) {
            throw new BusinessException("表映射关系ID不能为空！");
        }

        if (sourceTableName == null || sourceTableName.isEmpty()) {
            throw new BusinessException("源表名称不能为空！");
        }

        if (targetTableName == null || targetTableName.isEmpty()) {
            throw new BusinessException("目标表名称不能为空！");
        }

        if (fieldMappers == null || fieldMappers.size() == 0) {
            throw new BusinessException(String.format("源表：%s 与目标表：%s 未配置字段映射关系！", sourceTableName, targetTableName));
        }

        for (FieldMapper mapper : fieldMappers) {
            try {
                mapper.valid();
            } catch (Exception e) {
                throw new BusinessException(
                        String.format("源表：%s 与目标表：%s 字段映射错误！详细：%s", sourceTableName, targetTableName, e.getMessage()));
            }

        }
    }

    /**
     * 获取逻辑主键字段名称列表
     * 
     * @return 逻辑主键字段名称列表
     */
    public List<String> getSourceKeys() {
        List<String> result = new ArrayList<String>();
        for (FieldMapper fieldMapper : fieldMappers) {
            if (fieldMapper.getIsLogicKey()) {
                result.add(fieldMapper.getSourceFieldName());
            }
        }

        return result;
    }

}
