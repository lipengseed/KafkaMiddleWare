package org.nit.kafkaUtil.model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * 用于存储数据质量消费者配置文件
 * @author zjg
 * @date 2018/5/16
 */
public class ConsumerConfig {
    protected static Logger logger = LogManager.getLogger(ProducerConfig.class);
    /**
     * host/port列表，用于初始化建立和Kafka集群的连接。列表格式为host1:port1,host2:port2,....，无需添加所有的集群地址，kafka会根据提供的地址发现其他的地址
     */
    private String brokerList;
    /**
     *key的解析序列化接口实现类（Deserializer）。
     */
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    /**
     *value的解析序列化接口实现类（Deserializer）
     */
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    /**
     *服务器拉取请求返回的最小数据量，如果数据不足，请求将等待数据积累。
     * 默认设置为1字节，表示只要单个字节的数据可用或者读取等待请求超时，就会应答读取请求。
     * 将此值设置的越大将导致服务器等待数据累积的越长，这可能以一些额外延迟为代价提高服务器吞吐量。
     */
    private int fetchMinBytes = 1;
    /**
     *服务器拉取请求返回的最大数据值。
     * 这不是绝对的最大值，如果在第一次非空分区拉取的第一条消息大于该值，该消息将仍然返回，以确保消费者继续工作。
     * 接收的最大消息大小通过message.max.bytes (broker config) 或 max.message.bytes (topic config)定义。注意，消费者是并行执行多个提取的。
     */
    private int fetchMaxBytes = 52428800;
    /**
     *用于发现消费者故障的超时时间。
     * 消费者周期性的发送心跳到broker，表示其还活着。
     * 如果会话超时期满之前没有收到心跳，那么broker将从分组中移除消费者，并启动重新平衡。
     * 请注意，该值必须在broker配置的group.min.session.timeout.ms和group.max.session.timeout.ms允许的范围内。
     */
    private int sessionTimeoutMs = 1000;
    /**
     *当Kafka中没有初始offset或如果当前的offset不存在时（例如，该数据被删除了），该怎么办。
     earliest：自动将偏移重置为最早的偏移
     latest：自动将偏移重置为最新偏移
     none：如果消费者组找到之前的offset，则向消费者抛出异常
     其他：抛出异常给消费者。
     */
    private String autoOffsetReset = "latest";
    /**
     *如果为true，消费者的offset将在后台周期性的提交
     *如果为false，消费者的offset将由用户手动提交
     */
    private boolean enableAutoCommit = true;
    /**
     *如果enable.auto.commit设置为true，则消费者偏移量自动提交给Kafka的频率（以毫秒为单位）。
     */
    private int autoCommitIntervalMs = 5000;

    /**
     *在单次调用poll()中返回的最大记录数。
     */
//    private int maxPollRecords = 500;
    /**
     *服务器将返回每个分区的最大数据量
     */
    private int maxPartitionFetchBytes = 1048576;

    /**
     * 生产者生产的消息单条最大字节数
     */
    private int maxSingleMessageBytes = 5120;

    /**
     * 解析生产者配置文件
     * @param consumerConfigPath 配置文件的路径
     * @return 返回是否解析成功
     */
    public boolean parseConsumerConfig(String consumerConfigPath){
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(new File(consumerConfigPath));
            properties.load(inputStream);
            this.brokerList = properties.getProperty("broker_list");
            this.keyDeserializer = properties.getProperty("key_deserializer");
            this.valueDeserializer = properties.getProperty("value_deserializer");
            this.fetchMinBytes = Integer.parseInt(properties.getProperty("fetch_min_bytes"));
            this.fetchMaxBytes = Integer.parseInt(properties.getProperty("fetch_max_bytes"));
            this.sessionTimeoutMs = Integer.parseInt(properties.getProperty("session_timeout_ms"));
            this.autoOffsetReset = properties.getProperty("auto_offset_reset");
            this.enableAutoCommit= Boolean.getBoolean(properties.getProperty("enable_auto_commit"));
//            this.maxPollRecords = Integer.parseInt(properties.getProperty("max_poll_records"));
            this.autoCommitIntervalMs = Integer.parseInt(properties.getProperty("auto_commit_interval_ms"));
            this.maxPartitionFetchBytes = Integer.parseInt(properties.getProperty("max_partition_fetch_bytes"));
            this.maxSingleMessageBytes = Integer.parseInt(properties.getProperty("max_single_message_bytes"));
        } catch (FileNotFoundException e) {
            logger.error("Parse ProducerConfig: parse the producer.properties is error !" + e.toString(), e);
            return false;
        } catch (IOException e) {
            logger.error("Parse ProducerConfig: load the producer inputStream is error !" + e.toString(), e);
            return false;
        }
        return true;
    }
    public Properties propertiesConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", this.brokerList);
        properties.put("enable.auto.commit", this.enableAutoCommit);
        properties.put("auto.commit.interval.ms", this.autoCommitIntervalMs);
        properties.put("auto.offset.reset", this.autoOffsetReset);
        properties.put("key.deserializer", this.keyDeserializer);
        properties.put("value.deserializer", this.valueDeserializer);
        properties.put("session.timeout.ms", this.sessionTimeoutMs);
//        properties.put("max.poll.records", this.maxPollRecords);
        properties.put("fetch_Min_Bytes",this.fetchMinBytes);
        properties.put("fetchMaxBytes",this.fetchMaxBytes);
        properties.put("max.partition.fetch.bytes",this.maxPartitionFetchBytes);
        return properties;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public int getFetchMinBytes() {
        return fetchMinBytes;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

//    public int getMaxPollRecords() {
//        return maxPollRecords;
//    }

    public int getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public int getMaxSingleMessageBytes() {
        return maxSingleMessageBytes;
    }

    public int getMaxMessageNum(int partitionNum){
        int maxMessageNum = 0;
        if(fetchMaxBytes/partitionNum > maxPartitionFetchBytes){
            maxMessageNum = maxPartitionFetchBytes/maxSingleMessageBytes*partitionNum;
        }else{
            maxMessageNum = fetchMaxBytes/maxSingleMessageBytes;
        }
        return maxMessageNum;
    }
}
