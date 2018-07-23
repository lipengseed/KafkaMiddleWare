package org.nit.kafkaUtil.producer;

import org.apache.kafka.clients.producer.Producer;
import org.nit.kafkaUtil.model.DataQualityMessage;

/**
 * Created by Huangtt on 2018/5/7.
 */
public abstract class BaseProducer<P, M> {

    protected String producerPropsPath;

    protected String configTopicPath;

    /**
     * 生产者对象
     */
    protected Producer<P, Object> producer;

    /**
     * 对生产者进行初始化
     */
    public abstract void configure();

    /**
     * 对当前生产者的topic进行初始化
     */
    public abstract void manageTopic();

    /**
     * 异步的发送信息 String类型消息
     * @param protocolName 协议名
     * @param value 消息内容
     * @return 发送成功与否
     */
    public abstract boolean sendMessage(P protocolName, M value);

    /**
     * 关闭produce对象
     */
    public void close() {
        producer.close();
    }


}
