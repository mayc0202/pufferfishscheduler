package com.pufferfishscheduler.master.database.connect.nosql.impl;

import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.master.database.connect.nosql.AbstractNoSqlConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class RedisConnector extends AbstractNoSqlConnector {

    private JedisPool jedisPool;
    private JedisCluster jedisCluster;
    private JedisSentinelPool sentinelPool;

    private boolean isClusterMode = false;
    private boolean isSentinelMode = false;

    private static final int DEFAULT_TIMEOUT_MS = 2000;
    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final int DEFAULT_SCAN_COUNT = 1000;

    @Override
    public ConResponse connect() {
        ConResponse response = new ConResponse();
        try {
            initConnection();
            if (isClusterMode) {
                testClusterConnection(response);
            } else if (isSentinelMode) {
                testSentinelConnection(response);
            } else {
                testSingleConnection(response);
            }
        } catch (JedisConnectionException e) {
            log.error("Failed to connect to Redis", e);
            response.setResult(false);
            response.setMsg("连接失败: " + e.getMessage());
            throw new BusinessException("Redis连接失败！地址：" + buildAddressDescription() + "不可用！");
        } catch (Exception e) {
            log.error("Unexpected error during Redis connection", e);
            response.setResult(false);
            response.setMsg("连接失败: " + e.getMessage());
            throw new BusinessException("Redis连接失败: " + e.getMessage());
        }
        return response;
    }

    private void testClusterConnection(ConResponse response) {
        String testKey = "test_connection_" + UUID.randomUUID().toString().replace("-", "");
        jedisCluster.setex(testKey, 1, "test");
        String value = jedisCluster.get(testKey);
        if ("test".equals(value)) {
            Map<String, ConnectionPool> clusterNodes = jedisCluster.getClusterNodes();
            response.setResult(true);
            response.setMsg("连接成功！集群节点数：" + clusterNodes.size());
        } else {
            throw new BusinessException("Redis集群连接异常");
        }
    }

    private void testSentinelConnection(ConResponse response) {
        try (Jedis jedis = sentinelPool.getResource()) {
            if ("PONG".equals(jedis.ping())) {
                HostAndPort master = sentinelPool.getCurrentHostMaster();
                response.setResult(true);
                response.setMsg("连接成功！当前主节点：" + master.getHost() + ":" + master.getPort());
            } else {
                throw new BusinessException("Redis哨兵连接异常");
            }
        }
    }

    private void testSingleConnection(ConResponse response) {
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            if ("PONG".equals(pong)) {
                String info = jedis.info("server");
                String version = extractRedisVersion(info);
                response.setResult(true);
                response.setMsg("连接成功！Redis版本：" + version);
            } else {
                throw new BusinessException("Redis连接异常");
            }
        }
    }

    @Override
    public List<String> listKeys() {
        return listKeysWithPattern("*");
    }

    public List<String> listKeysWithPattern(String pattern) {
        List<String> keys = new ArrayList<>();
        try {
            initConnection();
            if (isClusterMode) {
                keys.addAll(scanKeysCluster(pattern));
            } else if (isSentinelMode) {
                try (Jedis jedis = sentinelPool.getResource()) {
                    selectDatabase(jedis);
                    keys.addAll(scanKeys(jedis, pattern));
                }
            } else {
                try (Jedis jedis = jedisPool.getResource()) {
                    selectDatabase(jedis);
                    keys.addAll(scanKeys(jedis, pattern));
                }
            }
            log.info("Retrieved {} keys with pattern: {}", keys.size(), pattern);
        } catch (Exception e) {
            log.error("Failed to list Redis keys", e);
            throw new BusinessException("获取Redis键列表失败: " + e.getMessage());
        }
        return keys;
    }

    /**
     * 集群 scan 终极修复：不再使用 Connection.scan
     */
    private List<String> scanKeysCluster(String pattern) {
        List<String> keys = new ArrayList<>();
        try {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams params = new ScanParams().match(pattern).count(DEFAULT_SCAN_COUNT);
            do {
                ScanResult<String> result = jedisCluster.scan(cursor, params);
                keys.addAll(result.getResult());
                cursor = result.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        } catch (Exception e) {
            log.warn("Cluster scan failed", e);
        }
        return keys;
    }

    private List<String> scanKeys(Jedis jedis, String pattern) {
        List<String> keys = new ArrayList<>();
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams().match(pattern).count(DEFAULT_SCAN_COUNT);
        do {
            ScanResult<String> result = jedis.scan(cursor, params);
            keys.addAll(result.getResult());
            cursor = result.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return keys;
    }

    @Override
    public long countKeys() {
        return countKeysWithPattern("*");
    }

    public long countKeysWithPattern(String pattern) {
        AtomicLong count = new AtomicLong(0);
        try {
            initConnection();
            if (isClusterMode) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams params = new ScanParams().match(pattern).count(DEFAULT_SCAN_COUNT);
                do {
                    ScanResult<String> result = jedisCluster.scan(cursor, params);
                    count.addAndGet(result.getResult().size());
                    cursor = result.getCursor();
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } else if (isSentinelMode) {
                try (Jedis jedis = sentinelPool.getResource()) {
                    selectDatabase(jedis);
                    count.set(scanCountKeys(jedis, pattern));
                }
            } else {
                try (Jedis jedis = jedisPool.getResource()) {
                    selectDatabase(jedis);
                    count.set(scanCountKeys(jedis, pattern));
                }
            }
            log.info("Total keys with pattern {}: {}", pattern, count.get());
        } catch (Exception e) {
            log.error("Failed to count Redis keys", e);
            throw new BusinessException("统计Redis键数量失败: " + e.getMessage());
        }
        return count.get();
    }

    private long scanCountKeys(Jedis jedis, String pattern) {
        long total = 0;
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams().match(pattern).count(DEFAULT_SCAN_COUNT);
        do {
            ScanResult<String> result = jedis.scan(cursor, params);
            total += result.getResult().size();
            cursor = result.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return total;
    }

    public long deleteKeys(String... keys) {
        if (keys == null || keys.length == 0) return 0;
        try {
            initConnection();
            long deleted;
            if (isClusterMode) {
                deleted = jedisCluster.del(keys);
            } else if (isSentinelMode) {
                try (Jedis jedis = sentinelPool.getResource()) {
                    selectDatabase(jedis);
                    deleted = jedis.del(keys);
                }
            } else {
                try (Jedis jedis = jedisPool.getResource()) {
                    selectDatabase(jedis);
                    deleted = jedis.del(keys);
                }
            }
            log.info("Deleted {} keys", deleted);
            return deleted;
        } catch (Exception e) {
            log.error("Failed to delete keys", e);
            throw new BusinessException("删除Redis键失败: " + e.getMessage());
        }
    }

    public long deleteKeysWithPattern(String pattern) {
        List<String> keys = listKeysWithPattern(pattern);
        if (keys.isEmpty()) return 0;
        return deleteKeys(keys.toArray(new String[0]));
    }

    public String getKeyType(String key) {
        try {
            initConnection();
            if (isClusterMode) {
                return jedisCluster.type(key);
            } else if (isSentinelMode) {
                try (Jedis jedis = sentinelPool.getResource()) {
                    selectDatabase(jedis);
                    return jedis.type(key);
                }
            } else {
                try (Jedis jedis = jedisPool.getResource()) {
                    selectDatabase(jedis);
                    return jedis.type(key);
                }
            }
        } catch (Exception e) {
            log.error("Failed to get type for key: {}", key, e);
            throw new BusinessException("获取键类型失败: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            try { jedisPool.close(); } catch (Exception e) { log.warn("Close jedisPool error", e); }
        }
        if (jedisCluster != null) {
            try { jedisCluster.close(); } catch (Exception e) { log.warn("Close jedisCluster error", e); }
        }
        if (sentinelPool != null) {
            try { sentinelPool.close(); } catch (Exception e) { log.warn("Close sentinelPool error", e); }
        }
    }

    private void initConnection() {
        if (jedisPool != null || jedisCluster != null || sentinelPool != null) return;
        String host = this.getDbHost();
        if (StringUtils.isBlank(host)) throw new BusinessException("Redis主机地址不能为空");

        if (host.contains(",") && !isSentinelConfig()) {
            isClusterMode = true;
            initClusterConnection();
        } else if (isSentinelConfig()) {
            isSentinelMode = true;
            initSentinelConnection();
        } else {
            initSingleConnection();
        }
    }

    private boolean isSentinelConfig() {
        Properties props = parseProperties();
        return props != null && "true".equals(props.getProperty("sentinel.enabled"));
    }

    private void initSingleConnection() {
        String host = this.getDbHost();
        int port = getPort();
        int timeout = getConnectionTimeout();
        String password = this.getPassword();
        int database = getDatabaseIndex();

        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(16);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(2);

        if (StringUtils.isNotBlank(password)) {
            jedisPool = new JedisPool(poolConfig, host, port, timeout, password, database);
        } else {
            jedisPool = new JedisPool(poolConfig, host, port, timeout, null, database);
        }
        log.info("Initialized Redis single node: {}:{}", host, port);
    }

    private void initClusterConnection() {
        String[] hosts = this.getDbHost().split(",");
        Set<HostAndPort> nodes = new HashSet<>();
        for (String hp : hosts) {
            String[] parts = hp.trim().split(":");
            String h = parts[0];
            int p = parts.length > 1 ? Integer.parseInt(parts[1]) : getPort();
            nodes.add(new HostAndPort(h, p));
        }
        int timeout = getConnectionTimeout();
        int maxAttempts = getMaxAttempts();
        String password = this.getPassword();

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(20);

        if (StringUtils.isNotBlank(password)) {
            jedisCluster = new JedisCluster(nodes, timeout, timeout, maxAttempts, password, poolConfig);
        } else {
            jedisCluster = new JedisCluster(nodes, timeout, timeout, maxAttempts, poolConfig);
        }
        log.info("Initialized Redis cluster with {} nodes", nodes.size());
    }

    private void initSentinelConnection() {
        String masterName = getSentinelMasterName();
        Set<String> sentinels = new HashSet<>();
        String[] hosts = this.getDbHost().split(",");
        for (String hp : hosts) {
            String[] parts = hp.trim().split(":");
            String h = parts[0];
            int p = parts.length > 1 ? Integer.parseInt(parts[1]) : getSentinelPort();
            sentinels.add(h + ":" + p);
        }
        int timeout = getConnectionTimeout();
        String password = this.getPassword();

        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();

        if (StringUtils.isNotBlank(password)) {
            sentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig, timeout, password);
        } else {
            sentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig, timeout);
        }
        log.info("Initialized Redis sentinel, master: {}", masterName);
    }

    private void selectDatabase(Jedis jedis) {
        int db = getDatabaseIndex();
        if (db > 0) jedis.select(db);
    }

    private String extractRedisVersion(String info) {
        if (StringUtils.isBlank(info)) return "unknown";
        for (String line : info.split("\n")) {
            if (line.startsWith("redis_version:")) return line.split(":")[1].trim();
        }
        return "unknown";
    }

    private int getPort() {
        return StringUtils.isNotBlank(this.getDbPort()) ? Integer.parseInt(this.getDbPort()) : 6379;
    }

    private int getConnectionTimeout() {
        Properties props = parseProperties();
        if (props != null && props.containsKey("connection.timeout")) {
            return Integer.parseInt(props.getProperty("connection.timeout"));
        }
        return DEFAULT_TIMEOUT_MS;
    }

    private int getMaxAttempts() {
        Properties props = parseProperties();
        if (props != null && props.containsKey("max.attempts")) {
            return Integer.parseInt(props.getProperty("max.attempts"));
        }
        return DEFAULT_MAX_ATTEMPTS;
    }

    private String getSentinelMasterName() {
        Properties props = parseProperties();
        if (props != null && props.containsKey("sentinel.master.name")) {
            return props.getProperty("sentinel.master.name");
        }
        return "mymaster";
    }

    private int getSentinelPort() {
        Properties props = parseProperties();
        if (props != null && props.containsKey("sentinel.port")) {
            return Integer.parseInt(props.getProperty("sentinel.port"));
        }
        return 26379;
    }

    @Override
    public Integer getDatabaseIndex() {
        Object idx = super.getDatabaseIndex();
        if (idx != null) {
            try {
                if (idx instanceof Integer) return (Integer) idx;
                return Integer.parseInt(idx.toString());
            } catch (NumberFormatException e) {
                log.warn("Invalid database index: {}", idx);
            }
        }
        return 0;
    }

    protected String buildAddressDescription() {
        return this.getDbHost();
    }
}