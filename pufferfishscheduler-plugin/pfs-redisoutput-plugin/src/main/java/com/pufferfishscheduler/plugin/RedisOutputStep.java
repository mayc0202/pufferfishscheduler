package com.pufferfishscheduler.plugin;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis输出步骤 - 将数据写入Redis
 * 支持单机和集群模式
 * 适配 Jedis 4.4.3 版本
 */
public class RedisOutputStep extends BaseStep implements StepInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOutputStep.class);
    private static final Class<?> PKG = RedisOutputStep.class;

    // 默认超时时间（毫秒）
    private static final int DEFAULT_TIMEOUT_MS = 2000;
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    private RedisOutputStepMeta meta;
    private RedisOutputStepData data;
    private JedisCluster jedisCluster;
    private Jedis jedis;
    private JedisPool jedisPool;

    public RedisOutputStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
                           int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        this.jedisCluster = null;
        this.jedis = null;
        this.jedisPool = null;
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (!super.init(smi, sdi)) {
            return false;
        }

        this.meta = (RedisOutputStepMeta) smi;
        this.data = (RedisOutputStepData) sdi;

        logDebug("Redis Output Step initialized successfully");
        return true;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
            throws KettleException, KettleStepException {

        this.meta = (RedisOutputStepMeta) smi;
        this.data = (RedisOutputStepData) sdi;

        Object[] currentRow = getRow();

        // 处理数据结束的情况（与 Redis 输入一致：首轮允许 currentRow==null，用于无上游时按配置写一次）
        if (shouldStopProcessing(currentRow)) {
            closeRedisConnections();
            setOutputDone();
            return false;
        }

        // 首次执行初始化
        if (first) {
            initializeFirstRow(currentRow);
        }

        // 获取Key和Value
        String redisKey = resolveRedisKey(currentRow);
        String redisValue = resolveRedisValue(currentRow);

        // 写入Redis
        writeToRedis(redisKey, redisValue);

        // 输出结果行（无上游无输入行时用空行占位，避免 putRow NPE）
        putRow(data.outputRowMeta, rowForDownstream(currentRow));
        incrementLinesOutput();

        // 输出进度日志
        logProgressIfNeeded();

        return true;
    }

    /**
     * 检查是否应停止处理数据行
     */
    private boolean shouldStopProcessing(Object[] currentRow) {
        return currentRow == null && !first;
    }

    private Object[] rowForDownstream(Object[] currentRow) {
        if (currentRow != null) {
            return currentRow;
        }
        return RowDataUtil.allocateRowData(data.outputRowMeta.size());
    }

    /**
     * 初始化首次执行的行数据
     * 包括克隆输入行元数据、获取字段索引、验证Redis连接等
     */
    private void initializeFirstRow(Object[] currentRow) throws KettleStepException {
        RowMetaInterface inputMeta = getInputRowMeta();
        if (inputMeta == null) {
            data.inputRowMeta = new RowMeta();
        } else {
            data.inputRowMeta = inputMeta;
        }
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields(data.outputRowMeta, getStepname(), null, null,
                this, repository, metaStore);

        // 确定Key和Value字段索引
        data.keyNr = data.inputRowMeta.indexOfValue(meta.getKey());
        data.valueNr = data.inputRowMeta.indexOfValue(meta.getValue());

        // 验证字段索引
        validateFieldIndexes();

        // 初始化Redis连接
        initializeRedisConnection();

        first = false;
    }

    /**
     * 验证 Key/Value 来源：输入字段或步骤中配置的常量（无上游时仅后者可用）
     */
    private void validateFieldIndexes() {
        boolean keyFromField = data.keyNr >= 0;
        boolean valueFromField = data.valueNr >= 0;
        if (!keyFromField && StringUtils.isBlank(meta.getKey())) {
            logError("未在输入中找到 Key 字段，且步骤未配置 Redis Key");
            setErrors(1L);
            stopAll();
            return;
        }
        if (!valueFromField && StringUtils.isBlank(meta.getValue())) {
            logError("未在输入中找到 Value 字段，且步骤未配置 Redis Value");
            setErrors(1L);
            stopAll();
        }
    }

    /**
     * 初始化Redis连接
     */
    private void initializeRedisConnection() {
        try {
            if (isClusterMode()) {
                jedisCluster = createJedisCluster(meta);
            } else {
                jedisPool = createJedisPool(meta);
                jedis = jedisPool.getResource();
                selectDbIfNeeded();
            }
        } catch (Exception e) {
            logError("Failed to initialize Redis connection", e);
            setErrors(1L);
            stopAll();
        }
    }

    /**
     * 检查是否为Redis集模式
     * 当主机名包含逗号时，返回true
     */
    private boolean isClusterMode() {
        return meta.getHost() != null && meta.getHost().contains(",");
    }

    /**
     * 选择Redis数据库
     */
    private void selectDbIfNeeded() {
        if (StringUtils.isNotBlank(meta.getDbName()) && jedis != null) {
            try {
                jedis.select(Integer.parseInt(meta.getDbName()));
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid database number: {}", meta.getDbName());
            }
        }
    }

    /**
     * 从当前行中获取Redis键
     */
    private String resolveRedisKey(Object[] currentRow) throws KettleValueException {
        if (data.keyNr < 0) {
            return environmentSubstitute(StringUtils.defaultString(meta.getKey()));
        }
        return environmentSubstitute(data.inputRowMeta.getString(currentRow, data.keyNr));
    }

    /**
     * 从当前行中获取Redis值
     */
    private String resolveRedisValue(Object[] currentRow) throws KettleValueException {
        if (data.valueNr < 0) {
            return environmentSubstitute(StringUtils.defaultString(meta.getValue()));
        }
        return environmentSubstitute(data.inputRowMeta.getString(currentRow, data.valueNr));
    }

    /**
     * 写入Redis键值对
     */
    private void writeToRedis(String key, String value) {
        if (key == null || key.isEmpty()) {
            logError("Redis key is empty, skipping write operation");
            setErrors(getErrors() + 1);
            return;
        }

        try {
            if (jedisCluster != null) {
                jedisCluster.set(key, value);
            } else if (jedis != null) {
                jedis.set(key, value);
            }

            if (log.isDebug()) {
                logDebug(String.format("Written to Redis - Key: %s, Value: %s",
                        truncateForLog(key), truncateForLog(value)));
            }
        } catch (Exception e) {
            logError("Failed to write to Redis, key: " + key, e);
            setErrors(getErrors() + 1);
            // 不抛出异常，继续处理下一条记录
        }
    }

    /**
     * 截断字符串以用于日志记录
     */
    private String truncateForLog(String str) {
        if (str == null) {
            return "null";
        }
        if (str.length() <= 100) {
            return str;
        }
        return str.substring(0, 97) + "...";
    }

    /**
     * 记录进度信息
     */
    private void logProgressIfNeeded() {
        if (checkFeedback(getLinesRead()) && log.isBasic()) {
            logBasic(BaseMessages.getString(PKG, "RowsFromResult.Log.LineNumber") + getLinesRead());
        }
    }

    /**
     * 关闭Redis连接
     */
    private void closeRedisConnections() {
        closeJedisQuietly();
        closeJedisPoolQuietly();
        closeJedisClusterQuietly();
    }

    /**
     * 关闭Jedis连接
     */
    private void closeJedisQuietly() {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                LOGGER.debug("Error closing Jedis connection", e);
            } finally {
                jedis = null;
            }
        }
    }

    /**
     * 关闭JedisPool
     */
    private void closeJedisPoolQuietly() {
        if (jedisPool != null) {
            try {
                jedisPool.close();
            } catch (Exception e) {
                LOGGER.debug("Error closing JedisPool", e);
            } finally {
                jedisPool = null;
            }
        }
    }

    /**
     * 关闭JedisCluster
     */
    private void closeJedisClusterQuietly() {
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
            } catch (Exception e) {
                LOGGER.debug("Error closing JedisCluster connection", e);
            } finally {
                jedisCluster = null;
            }
        }
    }

    /**
     * 创建 JedisPool - 适配 Jedis 4.4.3 API
     */
    private static JedisPool createJedisPool(RedisOutputStepMeta meta) {
        String host = meta.getHost();
        int port = Integer.parseInt(meta.getPort());

        JedisPoolConfig poolConfig = new JedisPoolConfig();

        // 根据是否有密码选择不同的构造函数
        if (StringUtils.isNotBlank(meta.getPassword())) {
            return new JedisPool(poolConfig, host, port, DEFAULT_TIMEOUT_MS, meta.getPassword());
        } else {
            return new JedisPool(poolConfig, host, port);
        }
    }

    /**
     * 创建 JedisCluster - 适配 Jedis 4.4.3 API
     */
    private static JedisCluster createJedisCluster(RedisOutputStepMeta meta) {
        Set<HostAndPort> nodes = new HashSet<>();
        String host = meta.getHost();
        int port = Integer.parseInt(meta.getPort());

        String[] hosts = host.split(",");
        for (String h : hosts) {
            nodes.add(new HostAndPort(h.trim(), port));
        }

        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();

        // 根据是否有密码选择不同的构造函数
        if (StringUtils.isNotBlank(meta.getPassword())) {
            return new JedisCluster(nodes, DEFAULT_TIMEOUT_MS, DEFAULT_TIMEOUT_MS,
                    DEFAULT_MAX_ATTEMPTS, meta.getPassword(), poolConfig);
        } else {
            return new JedisCluster(nodes, DEFAULT_TIMEOUT_MS, poolConfig);
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        closeRedisConnections();
        super.dispose(smi, sdi);
        logDebug("Redis Output Step disposed");
    }
}