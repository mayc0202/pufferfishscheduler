package com.pufferfishscheduler.cdc.kafka;

import com.pufferfishscheduler.common.constants.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 实时统计 Redis 写入器：统计消费者只负责解析 CDC 后调用本类写 Redis。
 * 累计量：rt:stats:task:{taskId}:table:{tableMapperId}（Hash：today_idv/udv/ddv, last_*, updated_at）
 * 按小时：rt:log:task:{taskId}:table:{tableMapperId}:{yyyyMMdd}:{HH}（Hash：insert/update/delete_data_volume），带 TTL。
 *
 * @author Mayc
 * @since 2026-03-15
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RealtimeStatsRedisWriter {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("HH");

    /** 按小时 key 的 TTL（天），避免 Redis 无限增长 */
    private static final long RT_LOG_TTL_DAYS = 7;

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 累计量 Redis Key
     */
    public static String buildStatsKey(Integer taskId, Integer tableMapperId) {
        return Constants.REDIS_KEY.RT_STATS_PREFIX + taskId + ":table:" + tableMapperId;
    }

    /**
     * 按小时日志 Redis Key
     */
    public static String buildLogKey(Integer taskId, Integer tableMapperId, int syncDate, int syncHour) {
        return Constants.REDIS_KEY.RT_LOG_PREFIX + taskId + ":table:" + tableMapperId + ":" + syncDate + ":" + syncHour;
    }

    /**
     * 单条写入：累计量 + 按小时量，并刷新 updated_at、设置按小时 key 的 TTL。
     *
     * @param taskId        任务ID
     * @param tableMapperId 表映射ID
     * @param syncDate      yyyyMMdd
     * @param syncHour      0~23
     * @param idv           插入条数
     * @param udv           更新条数
     * @param ddv           删除条数
     */
    public void increment(Integer taskId, Integer tableMapperId, int syncDate, int syncHour,
                          long idv, long udv, long ddv) {
        if (taskId == null || tableMapperId == null) {
            return;
        }
        String statsKey = buildStatsKey(taskId, tableMapperId);
        String logKey = buildLogKey(taskId, tableMapperId, syncDate, syncHour);
        String now = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        try {
            // 累计：today_*
            stringRedisTemplate.opsForHash().increment(statsKey, "today_idv", idv);
            stringRedisTemplate.opsForHash().increment(statsKey, "today_udv", udv);
            stringRedisTemplate.opsForHash().increment(statsKey, "today_ddv", ddv);
            stringRedisTemplate.opsForHash().put(statsKey, "updated_at", now);

            // 按小时
            stringRedisTemplate.opsForHash().increment(logKey, "insert_data_volume", idv);
            stringRedisTemplate.opsForHash().increment(logKey, "update_data_volume", udv);
            stringRedisTemplate.opsForHash().increment(logKey, "delete_data_volume", ddv);
            stringRedisTemplate.expire(logKey, RT_LOG_TTL_DAYS, TimeUnit.DAYS);
        } catch (Exception e) {
            log.error("RealtimeStatsRedisWriter increment failed, taskId={}, tableMapperId={}", taskId, tableMapperId, e);
        }
    }

    /**
     * 批量写入：先按 (taskId, tableMapperId, date, hour) 聚合同一批消息的 idv/udv/ddv，再一次性写 Redis（减少调用次数）。
     *
     * @param taskId 任务ID
     * @param batch  表名 -> (tableMapperId, syncDate, syncHour, idv, udv, ddv)
     */
    public void incrementBatch(Integer taskId, Map<String, RealtimeStatsIncrement> batch) {
        if (taskId == null || batch == null || batch.isEmpty()) {
            return;
        }
        for (RealtimeStatsIncrement inc : batch.values()) {
            increment(taskId, inc.getTableMapperId(), inc.getSyncDate(), inc.getSyncHour(),
                    inc.getIdv(), inc.getUdv(), inc.getDdv());
        }
    }

    /**
     * 单条增量信息，用于内存聚合后批量写 Redis
     */
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class RealtimeStatsIncrement {
        private Integer tableMapperId;
        private int syncDate;
        private int syncHour;
        private long idv;
        private long udv;
        private long ddv;

        public void add(long idv, long udv, long ddv) {
            this.idv += idv;
            this.udv += udv;
            this.ddv += ddv;
        }
    }
}
