package org.nit.kafkaUtil.model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * 用于存储数据质量生产者配置文件
 * @author kafkaTeam
 * @date 2018/5/8
 */
public class ProducerConfig {
    protected static Logger logger = LogManager.getLogger(ProducerConfig.class);
    /**
     * host/port列表，用于初始化建立和Kafka集群的连接。列表格式为host1:port1,host2:port2,....，无需添加所有的集群地址，kafka会根据提供的地址发现其他的地址
     */
    private String brokerList = "host-129-152:9092,host-129-153:9092";
    /**
     * 生产者需要leader确认请求完成之前接收的应答数。此配置控制了发送消息的耐用性，支持以下配置：
     * acks=0 如果设置为0，那么生产者将不等待任何消息确认。消息将立刻天际到socket缓冲区并考虑发送。在这种情况下不能保障消息被服务器接收到。并且重试机制不会生效（因为客户端不知道故障了没有）。每个消息返回的offset始终设置为-1。
     * acks=1，这意味着leader写入消息到本地日志就立即响应，而不等待所有follower应答。在这种情况下，如果响应消息之后但follower还未复制之前leader立即故障，那么消息将会丢失。
     * acks=all 这意味着leader将等待所有副本同步后应答消息。此配置保障消息不会丢失（只要至少有一个同步的副本或者）。这是最强壮的可用性保障。等价于acks=-1。
     */
    private String acks = "all";
    /**
     *当多个消息要发送到相同分区的时，生产者尝试将消息批量打包在一起，以减少请求交互。这样有助于客户端和服务端的性能提升。该配置的默认批次大小（以字节为单位）：
     *不会打包大于此配置大小的消息。
     *发送到broker的请求将包含多个批次，每个分区一个，用于发送数据。
     *较小的批次大小有可能降低吞吐量（批次大小为0则完全禁用批处理）。一个非常大的批次大小可能更浪费内存。因为我们会预先分配这个资源。
     */
    private Integer batchSize = 16384;
    /**
     *生产者用来缓存等待发送到服务器的消息的内存总字节数。如果消息发送比可传递到服务器的快，生产者将阻塞max.block.ms之后，抛出异常。
     *此设置应该大致的对应生产者将要使用的总内存，但不是硬约束，因为生产者所使用的所有内存都用于缓冲。一些额外的内存将用于压缩（如果启动压缩），以及用于保持发送中的请求。
     */
    private long bufferMemory = 33554432;
    /**
     *不是立即发出一个消息，生产者将等待一个给定的延迟，以便和其他的消息可以组合成一个批次。这类似于Nagle在TCP中的算法。此设置给出批量延迟的上限：一旦我们达到分区的batch.size值的记录，将立即发送，不管这个设置如何，但是，如果比这个小，我们将在指定的“linger”时间内等待更多的消息加入。此设置默认为0（即无延迟）。
     */
    private int lingerMs = 1;
    /**
     *key的序列化类（实现序列化接口）
     */
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    /**
     *value的序列化类（实现序列化接口）
     */
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    /**
     *数据压缩的类型。默认为空（就是不压缩）。有效的值有 none，gzip，snappy, 或 lz4。压缩全部的数据批，因此批的效果也将影响压缩的比率（更多的批次意味着更好的压缩）。
     */
    private String compressionType = "none";
    /**
     *该配置控制 KafkaProducer.send() 和 KafkaProducer.partitionsFor() 将阻塞多长时间。此外这些方法被阻止，也可能是因为缓冲区已满或元数据不可用。
     */
    private int maxBlockMs = 1000;

    /**
     * 解析生产者配置文件
     * @param producerConfigPath 配置文件的路径
     * @return 返回是否解析成功
     */
    public boolean parseProducerConfig(String producerConfigPath){
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(new File(producerConfigPath));
            properties.load(inputStream);
            this.brokerList = properties.getProperty("broker_list");
            this.acks = properties.getProperty("acks");
            this.batchSize = Integer.parseInt(properties.getProperty("batch_size"));
            this.bufferMemory = Long.parseLong(properties.getProperty("buffer_memory"));
            this.lingerMs = Integer.parseInt(properties.getProperty("linger_ms"));
            this.keySerializer = properties.getProperty("key_serializer");
            this.valueSerializer = properties.getProperty("value_serializer");
            this.compressionType= properties.getProperty("compression_type");
            this.maxBlockMs = Integer.parseInt(properties.getProperty("max_block_ms"));
        } catch (FileNotFoundException e) {
            logger.error("Parse ProducerConfig: parse the producer.properties is error !" + e.toString(), e);
            return false;
        } catch (IOException e) {
            logger.error("Parse ProducerConfig: load the producer inputStream is error !" + e.toString(), e);
            return false;
        } catch (Exception e){
            logger.error("Parse ProducerConfig  is error !" + e.toString(), e);
            return false;
        }
        return true;
    }

    /**
     * 将内容封装成properties
     * @return
     */
    public Properties propertiesConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", this.brokerList);
        properties.put("acks", this.acks);
        properties.put("retries", 3);
        properties.put("batch.size", this.batchSize);
        properties.put("linger.ms", this.lingerMs);
        properties.put("buffer.memory", this.bufferMemory);
        properties.put("key.serializer", this.keySerializer);
        properties.put("value.serializer", this.valueSerializer);
        properties.put("max.block.ms",this.maxBlockMs);
        return properties;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public String getAcks() {
        return acks;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public int getMaxBlockMs() {
        return maxBlockMs;
    }
}
