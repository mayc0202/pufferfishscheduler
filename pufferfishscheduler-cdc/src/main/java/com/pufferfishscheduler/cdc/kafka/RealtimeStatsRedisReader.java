package com.pufferfishscheduler.cdc.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 从 Redis 读取实时统计数据，供前端/API 优先使用（实时数据以 Redis 为准，历史/报表查 DB）。
 *
 * @author Mayc
 * @since 2026-03-15
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RealtimeStatsRedisReader {

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 读取某任务某表的累计统计（last_* + today_* + updated_at）。
     * 若 key 不存在或无数据则返回空 Map 或默认 0。
     */
    public Map<String, Object> getCumulativeStats(Integer taskId, Integer tableMapperId) {
        Map<String, Object> out = new HashMap<>();
        if (taskId == null || tableMapperId == null) {
            return out;
        }
        String key = RealtimeStatsRedisWriter.buildStatsKey(taskId, tableMapperId);
        try {
            Map<Object, Object> hash = stringRedisTemplate.opsForHash().entries(key);
            if (hash == null || hash.isEmpty()) {
                return out;
            }
            out.put("last_idv", parseLong(hash.get("last_idv"), 0L));
            out.put("last_udv", parseLong(hash.get("last_udv"), 0L));
            out.put("last_ddv", parseLong(hash.get("last_ddv"), 0L));
            out.put("today_idv", parseLong(hash.get("today_idv"), 0L));
            out.put("today_udv", parseLong(hash.get("today_udv"), 0L));
            out.put("today_ddv", parseLong(hash.get("today_ddv"), 0L));
            out.put("updated_at", hash.get("updated_at"));
        } catch (Exception e) {
            log.warn("getCumulativeStats failed, taskId={}, tableMapperId={}", taskId, tableMapperId, e);
        }
        return out;
    }

    /**
     * 读取某任务某表某小时的数据量（insert_data_volume, update_data_volume, delete_data_volume）。
     */
    public Map<String, Long> getHourlyStats(Integer taskId, Integer tableMapperId, int syncDate, int syncHour) {
        Map<String, Long> out = new HashMap<>();
        if (taskId == null || tableMapperId == null) {
            return out;
        }
        String key = RealtimeStatsRedisWriter.buildLogKey(taskId, tableMapperId, syncDate, syncHour);
        try {
            Object idv = stringRedisTemplate.opsForHash().get(key, "insert_data_volume");
            Object udv = stringRedisTemplate.opsForHash().get(key, "update_data_volume");
            Object ddv = stringRedisTemplate.opsForHash().get(key, "delete_data_volume");
            out.put("insert_data_volume", parseLong(idv, 0L));
            out.put("update_data_volume", parseLong(udv, 0L));
            out.put("delete_data_volume", parseLong(ddv, 0L));
        } catch (Exception e) {
            log.warn("getHourlyStats failed, taskId={}, tableMapperId={}, date={}, hour={}",
                    taskId, tableMapperId, syncDate, syncHour, e);
        }
        return out;
    }

    private static long parseLong(Object v, long defaultValue) {
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(v));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
