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
 * rt_table_stats
 * 表数据量统计
 * 每天结转一下这张表。last表示前一天的数据总量；today表示当天产生的数据总量。进入下一天时，上一天的总量=last_idv + today_idv
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "rt_table_stats")
public class RtTableStats implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键。与表rt_table_mapper表的主键ID一致
     */
    @TableId
    private Integer id;

    /**
     * 任务ID。rt_task表id
     */
    @TableField(value = "task_id")
    private Integer taskId;

    /**
     * 前一天插入数据量（last insert data volume）
     */
    @TableField(value = "last_idv")
    private Long lastIdv;

    /**
     * 前一天更新数据量（last update data volume）
     */
    @TableField(value = "last_udv")
    private Long lastUdv;

    /**
     * 前一天删除数据量（last delete data volume）
     */
    @TableField(value = "last_ddv")
    private Long lastDdv;

    /**
     * 当天插入数据量（today insert data volume）
     */
    @TableField(value = "today_idv")
    private Long todayIdv;

    /**
     * 当天更新数据量（today update data volume）
     */
    @TableField(value = "today_udv")
    private Long todayUdv;

    /**
     * 当天删除数据量（today delete data volume）
     */
    @TableField(value = "today_ddv")
    private Long todayDdv;

    /**
     * 更新时间
     */
    @TableField(value = "updated_time")
    private Date updatedTime;

}
