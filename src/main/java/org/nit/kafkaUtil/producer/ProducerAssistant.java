package org.nit.kafkaUtil.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.nit.kafkaUtil.model.DataQualityMessage;
import org.nit.kafkaUtil.model.TopicConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 考虑到后续可能多种类型的produce 都会调用一些通用的方法，封装成一个工具类
 * @author kafkaTeam
 * @date 2018/5/8
 */
public class ProducerAssistant {
    protected static Logger logger = LogManager.getLogger(ProducerAssistant.class);

    /**
     * 发送消息
     * @param producer
     * @param record
     */
    public static void sendMessage(Producer<String, Object> producer, ProducerRecord<String, Object> record){
            producer.send(record,
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                logger.error(e.toString(), e);
                            } else {
                                logger.info("The topic of the record we just sent is: " + metadata.topic() + " The offset is: " + metadata.offset() + ";The partition is:" + metadata.partition());
                            }
                        }
                    }
            );
    }





    /**
     * 通过协议的名称获得对应的partition
     * @param topicConfigList
     * @param protocolName
     * @return
     */
    public static Integer getPartitionKey(List<TopicConfig> topicConfigList, String protocolName){
        Integer partitionKey = -1;
        for (TopicConfig topicConfig : topicConfigList){
            Map<String, List<Integer>> protocolPartition = topicConfig.getProtocolPartion();
            for (Map.Entry<String, List<Integer>> entry : protocolPartition.entrySet()){
                if (protocolName.equalsIgnoreCase(entry.getKey())){
                    partitionKey = entry.getValue().get((int) (Math.random() * entry.getValue().size()));
                }
            }
        }
        return partitionKey;
    }

}
