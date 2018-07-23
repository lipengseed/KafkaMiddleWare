package org.nit.kafkaUtil.cousumer;


import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

/**
 * @author zjg
 * @date 2018/5/16
 */
public abstract class BaseConsumer<P, M>{
    protected String consumerPropsPath;

    protected String configTopicPath;

    protected int messageNum;

    protected long pollTimeout;

    /**
     * 消费者对象
     */
    protected KafkaConsumer<P, M> consumer;

    /**
     * 对消费者进行初始化
     * @param groupId 所属消费者组信息
     * @param topicList topic 列表
     * @return 返回实际的消费数量
     */
    public abstract int configure(String groupId, List<String> topicList);


    /**
     * 获取消息
     * @return 消息集合
     */
    public abstract List<M> getMessage(int minBatchSize, long timeout);

    /**
     * 提交偏移量
     */
    public abstract void commitOffset();

}
