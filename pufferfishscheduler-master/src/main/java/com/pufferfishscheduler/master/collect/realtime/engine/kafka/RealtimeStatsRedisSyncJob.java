package com.pufferfishscheduler.master.collect.realtime.engine.kafka;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.pufferfishscheduler.common.utils.CommonUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.dao.entity.RtSyncLog;
import com.pufferfishscheduler.dao.entity.RtTableStats;
import com.pufferfishscheduler.dao.mapper.RtSyncLogMapper;
import com.pufferfishscheduler.dao.mapper.RtTableStatsMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 定时将 Redis 中的实时统计数据同步入库。
 * 累计量 → rt_table_stats（跨天时今日→昨日结转）；按小时 → rt_sync_log。
 * 建议每 5~15 分钟执行一次。
 *
 * @author Mayc
 * @since 2026-03-15
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RealtimeStatsRedisSyncJob {

    private static final Pattern STATS_KEY_PATTERN = Pattern
            .compile("^rt:stats:task:(\\d+):table:(\\d+)$");
    private static final Pattern LOG_KEY_PATTERN = Pattern
            .compile("^rt:log:task:(\\d+):table:(\\d+):(\\d{8}):(\\d{1,2})$");

    private final StringRedisTemplate stringRedisTemplate;
    private final RtTableStatsMapper rtTableStatsMapper;
    private final RtSyncLogMapper rtSyncLogMapper;

    /**
     * 每 5 分钟同步一次 Redis 统计到 DB
     */
    @Scheduled(fixedDelayString = "${realtime.stats.sync.fixed-delay-ms:300000}")
    public void syncRedisToDb() {
        try {
            syncCumulativeStats();
            syncHourlyLogs();
        } catch (Exception e) {
            log.error("RealtimeStatsRedisSyncJob sync failed", e);
        }
    }

    /**
     * 累计量：扫描 rt:stats:task:*，读 Hash 写入/更新 rt_table_stats；跨天时做今日→昨日结转。
     */
    protected void syncCumulativeStats() {
        String pattern = Constants.REDIS_KEY.RT_STATS_PREFIX + "*";
        Set<String> keys = scanKeys(pattern);
        int today = getTodayDate();
        Date now = new Date();

        for (String key : keys) {
            try {
                Matcher m = STATS_KEY_PATTERN.matcher(key);
                if (!m.matches()) {
                    continue;
                }
                int taskId = Integer.parseInt(m.group(1));
                int tableMapperId = Integer.parseInt(m.group(2));

                String updatedAtStr = (String) stringRedisTemplate.opsForHash().get(key, "updated_at");
                long lastIdv = parseLong(stringRedisTemplate.opsForHash().get(key, "last_idv"), 0L);
                long lastUdv = parseLong(stringRedisTemplate.opsForHash().get(key, "last_udv"), 0L);
                long lastDdv = parseLong(stringRedisTemplate.opsForHash().get(key, "last_ddv"), 0L);
                long todayIdv = parseLong(stringRedisTemplate.opsForHash().get(key, "today_idv"), 0L);
                long todayUdv = parseLong(stringRedisTemplate.opsForHash().get(key, "today_udv"), 0L);
                long todayDdv = parseLong(stringRedisTemplate.opsForHash().get(key, "today_ddv"), 0L);

                // 跨天结转：若 updated_at 的日期小于今天，则 today 并入 last，today 清 0
                int updatedDate = parseDateFromUpdatedAt(updatedAtStr);
                if (updatedDate > 0 && updatedDate < today) {
                    lastIdv += todayIdv;
                    lastUdv += todayUdv;
                    lastDdv += todayDdv;
                    todayIdv = 0;
                    todayUdv = 0;
                    todayDdv = 0;
                    stringRedisTemplate.opsForHash().put(key, "last_idv", String.valueOf(lastIdv));
                    stringRedisTemplate.opsForHash().put(key, "last_udv", String.valueOf(lastUdv));
                    stringRedisTemplate.opsForHash().put(key, "last_ddv", String.valueOf(lastDdv));
                    stringRedisTemplate.opsForHash().put(key, "today_idv", "0");
                    stringRedisTemplate.opsForHash().put(key, "today_udv", "0");
                    stringRedisTemplate.opsForHash().put(key, "today_ddv", "0");
                }

                RtTableStats stats = rtTableStatsMapper.selectByTableMapperId(tableMapperId);
                if (stats == null) {
                    stats = RtTableStats.builder()
                            .id(tableMapperId)
                            .taskId(taskId)
                            .lastIdv(lastIdv)
                            .lastUdv(lastUdv)
                            .lastDdv(lastDdv)
                            .todayIdv(todayIdv)
                            .todayUdv(todayUdv)
                            .todayDdv(todayDdv)
                            .updatedTime(now)
                            .build();
                    rtTableStatsMapper.insert(stats);
                } else {
                    stats.setLastIdv(lastIdv);
                    stats.setLastUdv(lastUdv);
                    stats.setLastDdv(lastDdv);
                    stats.setTodayIdv(todayIdv);
                    stats.setTodayUdv(todayUdv);
                    stats.setTodayDdv(todayDdv);
                    stats.setUpdatedTime(now);
                    rtTableStatsMapper.updateById(stats);
                }
            } catch (Exception e) {
                log.warn("Sync cumulative stats failed for key: {}", key, e);
            }
        }
    }

    /**
     * 按小时：扫描 rt:log:task:*，只处理“已过去”的小时，写入 rt_sync_log。
     */
    protected void syncHourlyLogs() {
        String pattern = Constants.REDIS_KEY.RT_LOG_PREFIX + "*";
        Set<String> keys = scanKeys(pattern);
        int today = getTodayDate();
        int currentHour = java.time.LocalDateTime.now().getHour();

        for (String key : keys) {
            try {
                Matcher m = LOG_KEY_PATTERN.matcher(key);
                if (!m.matches()) {
                    continue;
                }
                int taskId = Integer.parseInt(m.group(1));
                int tableMapperId = Integer.parseInt(m.group(2));
                int syncDate = Integer.parseInt(m.group(3));
                int syncHour = Integer.parseInt(m.group(4));

                // 只同步已过去的小时
                if (syncDate > today) {
                    continue;
                }
                if (syncDate == today && syncHour >= currentHour) {
                    continue;
                }

                long idv = parseLong(stringRedisTemplate.opsForHash().get(key, "insert_data_volume"), 0L);
                long udv = parseLong(stringRedisTemplate.opsForHash().get(key, "update_data_volume"), 0L);
                long ddv = parseLong(stringRedisTemplate.opsForHash().get(key, "delete_data_volume"), 0L);

                LambdaQueryWrapper<RtSyncLog> q = new LambdaQueryWrapper<>();
                q.eq(RtSyncLog::getTaskId, taskId)
                        .eq(RtSyncLog::getSyncDate, syncDate)
                        .eq(RtSyncLog::getSyncHour, syncHour)
                        .eq(RtSyncLog::getTableMapperId, tableMapperId);
                RtSyncLog existing = rtSyncLogMapper.selectOne(q);

                Date now = new Date();
                if (existing == null) {
                    RtSyncLog logEntity = RtSyncLog.builder()
                            .id(CommonUtil.getUUIDString())
                            .taskId(taskId)
                            .syncDate(syncDate)
                            .syncHour(syncHour)
                            .tableMapperId(tableMapperId)
                            .insertDataVolume(idv)
                            .updateDataVolume(udv)
                            .deleteDataVolume(ddv)
                            .createdBy(Constants.SYS_OP_INFO.SYSTEM_ACCOUNT)
                            .createdTime(now)
                            .build();
                    rtSyncLogMapper.insert(logEntity);
                } else {
                    existing.setInsertDataVolume(idv);
                    existing.setUpdateDataVolume(udv);
                    existing.setDeleteDataVolume(ddv);
                    rtSyncLogMapper.updateById(existing);
                }

                // 已同步的可删除 Redis key，避免重复处理（可选，也可依赖 TTL）
                stringRedisTemplate.delete(key);
            } catch (Exception e) {
                log.warn("Sync hourly log failed for key: {}", key, e);
            }
        }
    }

    private Set<String> scanKeys(String pattern) {
        Set<String> keys = stringRedisTemplate.keys(pattern);
        return keys != null ? keys : new HashSet<>();
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

    /** 从 updated_at 字符串（如 2026-03-15T12:30:00）解析出 yyyyMMdd */
    private static int parseDateFromUpdatedAt(String updatedAtStr) {
        if (updatedAtStr == null || updatedAtStr.length() < 10) {
            return 0;
        }
        try {
            // 取 "yyyy-MM-dd"
            String datePart = updatedAtStr.substring(0, 10).replace("-", "");
            return Integer.parseInt(datePart);
        } catch (Exception e) {
            return 0;
        }
    }

    private static int getTodayDate() {
        return LocalDateTime.now().getYear() * 10000
                + LocalDateTime.now().getMonthValue() * 100
                + LocalDateTime.now().getDayOfMonth();
    }
}
