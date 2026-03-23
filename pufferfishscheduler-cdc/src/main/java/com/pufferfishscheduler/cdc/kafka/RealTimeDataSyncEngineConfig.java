package com.pufferfishscheduler.cdc.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 实时数据同步引擎配置
 */
@Component
public class RealTimeDataSyncEngineConfig {
    /**
     * 主题数据复制数量
     */
    @Value("${realtime.topic.replication.factor}")
    public String topicReplicationFactor = "1";

    /**
     * 分区数量（按照kafka集群机器数量分配。提升并发写性能）
     */
    @Value("${realtime.topic.partitions}")
    public String topicPartitions = "1";

    /**
     * 主题中，消息压缩算法。可以节省网络带宽
     */
    @Value("${realtime.topic.compression.type}")
    public String topicCompressionType = "snappy";

    /**
     * 消息写入成功反馈方式。可取值：
     * 1 - 只要分区中的leader副本成功写入消息，那么就会接收到服务器的成功响应；
     * 0 - 生产者把消息投递出去及算成功，不管消息是否写入成功；
     * -1 - 生产者需要等待ISR中所有的副本都成功写入消息之后才能够收到服务器端的成功响应；
     */
    @Value("${realtime.topic.acks}")
    public String topicAcks = "1";

    /**
     * 消费者每次poll最大拉取消息条数
     */
    @Value("${realtime.consumer.max.poll.records}")
    public String consumerMaxPollRecords = "10000";

    /**
     * 从每个分区每次返回给消费者最大消费字节数（100MB）
     */
    @Value("${realtime.consumer.max.partition.fetch.bytes}")
    public String consumerMaxPartitionFetchBytes = "10485760";

    /**
     * 每次最大获取的记录大小（50MB）
     */
    @Value("${realtime.consumer.fetch.min.bytes}")
    public String consumerFetchMinBytes = "5242880";

    /**
     * 消费端压缩算法
     */
    @Value("${realtime.consumer.compression.type}")
    public String consumerCompressionType = "snappy";

    /**
     * 消费端压缩算法
     */
    @Value("${realtime.consumer.fetch.max.wait.ms}")
    public String consumerFetchMaxWaitMs = "2000";
}
