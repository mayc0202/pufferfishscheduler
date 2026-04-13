package com.pufferfishscheduler.plugin;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

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
import redis.clients.jedis.resps.Tuple;

/**
 * Redis输入步骤 - 从Redis读取数据
 * 支持单机和集群模式，支持String、Hash、List、Set、ZSet数据类型
 * 适配 Jedis 4.4.3 版本
 */
public class RedisInputStep extends BaseStep implements StepInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisInputStep.class);
    private static final Class<?> PKG = RedisInputStep.class;

    // Redis数据类型常量
    private static final String TYPE_STRING = "string";
    private static final String TYPE_HASH = "hash";
    private static final String TYPE_LIST = "list";
    private static final String TYPE_SET = "set";
    private static final String TYPE_ZSET = "zset";
    private static final String TYPE_NONE = "none";

    // 输出字段数量
    private static final int EXTRA_FIELDS_COUNT = 2;

    // 分隔符常量
    private static final String KEY_VALUE_SEPARATOR = "=";
    private static final String EMPTY_STRING = "";

    // 默认超时时间（毫秒）
    private static final int DEFAULT_TIMEOUT_MS = 2000;
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    private RedisInputStepMeta meta;
    private RedisInputStepData data;
    private JedisCluster jedisCluster;
    private Jedis jedis;
    private JedisPool jedisPool;

    public RedisInputStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
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

        this.meta = (RedisInputStepMeta) smi;
        this.data = (RedisInputStepData) sdi;

        logDebug("Redis Input Step initialized successfully");
        return true;
    }

    /**
     * 处理Redis输入步骤的一行数据。
     *
     * @param smi 步骤元数据接口
     * @param sdi 步骤数据接口
     * @return 是否继续处理下一行数据
     * @throws KettleException     如果处理过程中发生错误
     * @throws KettleStepException 如果处理过程中发生错误
     */
    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
            throws KettleException, KettleStepException {

        this.meta = (RedisInputStepMeta) smi;
        this.data = (RedisInputStepData) sdi;

        Object[] currentRow = getRow();

        // 处理数据结束的情况
        if (shouldStopProcessing(currentRow)) {
            closeRedisConnections();
            setOutputDone();
            return false;
        }

        // 首次执行初始化
        if (first) {
            initializeFirstRow(currentRow);
        }

        // 获取Redis Key
        String redisKey = resolveRedisKey(currentRow);

        // 读取Redis数据
        String redisValue = readRedisValue(redisKey);

        // 构建并输出结果行
        Object[] resultRow = buildResultRow(currentRow, redisKey, redisValue);
        putRow(data.outputRowMeta, resultRow);

        // 输出进度日志
        logProgressIfNeeded();

        return true;
    }

    /**
     * 判断是否应该停止处理当前行数据。
     *
     * @param currentRow 当前行数据
     * @return 是否应该停止处理当前行数据
     */
    private boolean shouldStopProcessing(Object[] currentRow) {
        return currentRow == null && !first;
    }

    /**
     * 初始化首次执行的行数据。
     *
     * @param currentRow 当前行数据
     * @throws KettleStepException 如果初始化过程中发生错误
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

        // 确定Key字段索引
        if (currentRow == null) {
            data.inStreamNr = -1;
        } else {
            data.inStreamNr = data.inputRowMeta.indexOfValue(meta.getKey());
        }

        // 初始化Redis连接
        initializeRedisConnection();

        first = false;
    }

    /**
     * 初始化Redis连接。
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

    private boolean isClusterMode() {
        return meta.getHost() != null && meta.getHost().contains(",");
    }

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
     * 从当前行数据中解析Redis Key。
     *
     * @param currentRow 当前行数据
     * @return 解析后的Redis Key
     * @throws KettleValueException 如果解析过程中发生错误
     */
    private String resolveRedisKey(Object[] currentRow) throws KettleValueException {
        if (data.inStreamNr < 0) {
            return environmentSubstitute(meta.getKey());
        }
        return data.inputRowMeta.getString(currentRow, data.inStreamNr);
    }

    /**
     * 从Redis读取值。
     *
     * @param key Redis Key
     * @return 从Redis读取到的值
     */
    private String readRedisValue(String key) {
        if (key == null || key.isEmpty()) {
            return EMPTY_STRING;
        }

        try {
            if (jedisCluster != null) {
                return readFromCluster(key);
            } else if (jedis != null) {
                return readFromStandalone(key);
            }
        } catch (Exception e) {
            logError("Failed to read from Redis, key: " + key, e);
            // 使用 setErrors 增加错误计数
            setErrors(getErrors() + 1);
        }

        return EMPTY_STRING;
    }

    /**
     * 从Redis集群读取值。
     *
     * @param key Redis Key
     * @return 从Redis集群读取到的值
     */
    private String readFromCluster(String key) {
        String dataType = jedisCluster.type(key);
        return readByDataType(dataType, key, true);
    }

    /**
     * 从Redis独立实例读取值。
     *
     * @param key Redis Key
     * @return 从Redis独立实例读取到的值
     */
    private String readFromStandalone(String key) {
        String dataType = jedis.type(key);
        return readByDataType(dataType, key, false);
    }

    /**
     * 根据Redis数据类型读取值。
     *
     * @param dataType  Redis数据类型
     * @param key       Redis Key
     * @param isCluster 是否为集群模式
     * @return 从Redis读取到的值
     */
    private String readByDataType(String dataType, String key, boolean isCluster) {
        if (TYPE_NONE.equals(dataType)) {
            LOGGER.debug("Redis key does not exist: {}", key);
            return EMPTY_STRING;
        }

        return switch (dataType) {
            case TYPE_STRING -> readStringValue(key, isCluster);
            case TYPE_HASH -> readHashValue(key, isCluster);
            case TYPE_LIST -> readListValue(key, isCluster);
            case TYPE_SET -> readSetValue(key, isCluster);
            case TYPE_ZSET -> readZSetValue(key, isCluster);
            default -> {
                LOGGER.debug("Unknown Redis data type: {} for key: {}", dataType, key);
                yield EMPTY_STRING;
            }
        };
    }

    private String readStringValue(String key, boolean isCluster) {
        String value = isCluster ? jedisCluster.get(key) : jedis.get(key);
        return value != null ? value : EMPTY_STRING;
    }

    private String readHashValue(String key, boolean isCluster) {
        Map<String, String> hashData = isCluster ?
                jedisCluster.hgetAll(key) : jedis.hgetAll(key);

        if (hashData == null || hashData.isEmpty()) {
            return EMPTY_STRING;
        }

        StringJoiner joiner = new StringJoiner(meta.getFieldDelimiter());
        for (Map.Entry<String, String> entry : hashData.entrySet()) {
            joiner.add(entry.getKey() + KEY_VALUE_SEPARATOR + entry.getValue());
        }
        return joiner.toString();
    }

    private String readListValue(String key, boolean isCluster) {
        List<String> listData = isCluster ?
                jedisCluster.lrange(key, 0, -1) : jedis.lrange(key, 0, -1);

        return joinCollection(listData, meta.getFieldDelimiter());
    }

    private String readSetValue(String key, boolean isCluster) {
        Set<String> setData = isCluster ?
                jedisCluster.smembers(key) : jedis.smembers(key);

        return joinCollection(setData, meta.getFieldDelimiter());
    }

    private String readZSetValue(String key, boolean isCluster) {
        List<Tuple> zsetData = isCluster ?
                jedisCluster.zrangeWithScores(key, 0, -1) : jedis.zrangeWithScores(key, 0, -1);

        if (zsetData == null || zsetData.isEmpty()) {
            return EMPTY_STRING;
        }

        StringJoiner joiner = new StringJoiner(meta.getFieldDelimiter());
        for (Tuple tuple : zsetData) {
            joiner.add(tuple.getElement());
        }
        return joiner.toString();
    }

    private String joinCollection(Iterable<String> collection, String delimiter) {
        if (collection == null || !collection.iterator().hasNext()) {
            return EMPTY_STRING;
        }
        return String.join(delimiter, collection);
    }

    private Object[] buildResultRow(Object[] currentRow, String key, String value) {
        Object[] extraFields = {key, value};
        Object[] result = resizeArray(currentRow, data.inputRowMeta.size() + EXTRA_FIELDS_COUNT);
        System.arraycopy(extraFields, 0, result, data.inputRowMeta.size(), extraFields.length);
        return result;
    }

    private void logProgressIfNeeded() {
        if (checkFeedback(getLinesRead()) && log.isBasic()) {
            logBasic(BaseMessages.getString(PKG, "RowsFromResult.Log.LineNumber") + getLinesRead());
        }
    }

    private void closeRedisConnections() {
        closeJedisQuietly();
        closeJedisPoolQuietly();
        closeJedisClusterQuietly();
    }

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
     * 关闭JedisPool连接。
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
     * 关闭JedisCluster连接。
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
     * 为数组分配新的大小，保留原始数组的内容。
     *
     * @param original 原始数组
     * @param newSize  新数组大小
     * @return 新数组
     */
    public static Object[] resizeArray(Object[] original, int newSize) {
        if (original != null && original.length >= newSize) {
            return original;
        }

        Object[] newArray = new Object[newSize + RowDataUtil.OVER_ALLOCATE_SIZE];
        if (original != null) {
            System.arraycopy(original, 0, newArray, 0, original.length);
        }
        return newArray;
    }

    /**
     * 创建 JedisPool - 适配 Jedis 4.4.3 API
     */
    private static JedisPool createJedisPool(RedisInputStepMeta meta) {
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
    private static JedisCluster createJedisCluster(RedisInputStepMeta meta) {
        Set<HostAndPort> nodes = new HashSet<>();
        String host = meta.getHost();
        int port = Integer.parseInt(meta.getPort());

        String[] hosts = host.split(",");
        for (String h : hosts) {
            nodes.add(new HostAndPort(h.trim(), port));
        }

        // 使用 GenericObjectPoolConfig<Connection>
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
        logDebug("Redis Input Step disposed");
    }
}