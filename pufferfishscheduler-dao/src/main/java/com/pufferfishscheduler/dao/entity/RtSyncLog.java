package com.pufferfishscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * rt_sync_log
 * 数据同步日志表（记录每个小时的数据同步情况）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rt_sync_log")
public class RtSyncLog implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键。UUID
     */
    @TableId
    private String id;

    /**
     * 任务ID
     */
    @TableField(value = "task_id")
    private Integer taskId;

    /**
     * 同步时间。记录格式：yyyyMMdd
     */
    @TableField(value = "sync_date")
    private Integer syncDate;

    /**
     * 同步小时。同一天中，那个小时产生的日志。0~23
     */
    @TableField(value = "sync_hour")
    private Integer syncHour;

    /**
     * rt_table_mapper表的id
     */
    @TableField(value = "table_mapper_id")
    private Integer tableMapperId;

    /**
     * 同步数据量（插入）
     */
    @TableField(value = "insert_data_volume")
    private Long insertDataVolume;

    /**
     * 同步数据量（更新）
     */
    @TableField(value = "update_data_volume")
    private Long updateDataVolume;

    /**
     * 同步数据量（删除）
     */
    @TableField(value = "delete_data_volume")
    private Long deleteDataVolume;

    /**
     * 创建人账号
     */
    @TableField(value = "created_by")
    private String createdBy;

    /**
     * 创建时间
     */
    @TableField(value = "created_time")
    private Date createdTime;

}
