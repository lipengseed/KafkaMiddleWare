package org.nit.kafkaUtil.cousumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.nit.kafkaUtil.model.SystemConfig;
import org.nit.kafkaUtil.model.TopicConfig;

import java.util.List;
import java.util.Map;


/**
 * 考虑到后续可能多种类型的consumer都会调用一些通用的方法，封装成一个工具类
 * @author zjg
 * @date 2018/5/16
 */
public class ConsumerAssistant {
    protected static Logger logger = LogManager.getLogger(ConsumerAssistant.class);

    /**
     * 拉取消息方法
     * @param consumer 消费者
     * @return 拉取的消息集合
     */
    public static ConsumerRecords<String, String> getMessage(KafkaConsumer<String, String> consumer, long pollTimeout){
        ConsumerRecords<String, String> records = null;
        try {
            records = consumer.poll(pollTimeout);
        }catch(Exception e){
            logger.error("poll message occurs error:" + e.toString());
        }
        return records;
    }
    /**
     * 同步提交偏移量方法
     * @param consumer 消费者
     * @param currentOffsets 偏移量信息
     */
    public static void commitSyncOffset(KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets){
        try{
            consumer.commitSync(currentOffsets);
        }catch(Exception e){
            logger.error("commit offset occurs error:" + e.toString());
        }
    }
    /**
     * 异步提交偏移量方法
     * @param consumer 消费者
     * @param currentOffsets 偏移量信息
     */
    public static void commitAsyncOffset(KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets){
        try{
            consumer.commitAsync(currentOffsets, null);
        }catch(Exception e){
            logger.error("commit offset occurs error:" + e.toString());
        }
    }

    /**
     * 判断是否所有的topic是否都在xml中存在
     * @param topicConfigList xml中topic的列表
     * @param topicNameList 调用者topic的列表
     * @return
     */
    public static boolean isExistTopic(List<TopicConfig> topicConfigList, List<String> topicNameList){

        for (String topicName : topicNameList){
            boolean isExistTopic = false;
            for (TopicConfig topicConfig : topicConfigList){
                if (topicName.equalsIgnoreCase(topicConfig.getTopicName())){
                    isExistTopic = true;
                }
            }
            if (!isExistTopic){
                return false;
            }
        }

        return true;
    }

}
