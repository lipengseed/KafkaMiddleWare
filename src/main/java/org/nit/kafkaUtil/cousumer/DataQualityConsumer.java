package org.nit.kafkaUtil.cousumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.nit.kafkaUtil.model.ConsumerConfig;
import org.nit.kafkaUtil.model.TopicConfig;
import org.nit.kafkaUtil.util.readConfig.ReadTopicConfig;
import org.nit.kafkaUtil.util.topicManage.TopicAssistant;

import java.util.*;

/**
 * 数据管理消费者
 *
 * @author zjg
 * @date 2018/5/16
 */
public class DataQualityConsumer extends BaseConsumer<String, String> {
    protected static Logger logger = LogManager.getLogger(DataQualityConsumer.class);

    public DataQualityConsumer(String consumerPropsPath, String configTopicPath) {
        this.consumerPropsPath = consumerPropsPath;
        this.configTopicPath = configTopicPath;
    }

    @Override
    public int configure(String groupId, List<String> topicList) {
        return 0;
    }

    @Override
    public List<String> getMessage(int minBatchSize, long timeout) {
        return null;
    }

    @Override
    public void commitOffset() {

    }

    /*@Override
    public int configure(String groupId, String protocolName, int num) {
        List<TopicConfig> topicConfigList = ReadTopicConfig.getInstance(configTopicPath).getTopicConfigsList();
        String topic = TopicAssistant.getTopicName(topicConfigList, protocolName);
        if ("".equals(topic)) {
            logger.error("Send Message is Error ! Not Found Topic Name");
        }
        int partitionNum = TopicAssistant.getTopicPartitionNum(topicConfigList, topic);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.parseConsumerConfig(consumerPropsPath);
        this.messageNum = consumerConfig.getMaxMessageNum(partitionNum);
        if (messageNum <= num) {
            logger.info("Single pull message cannot exceed" + messageNum);
        } else {
            messageNum = num;
        }
        System.out.println("partitionNum:" + partitionNum + " MessageNum:" + messageNum);
        Properties properties = consumerConfig.propertiesConfig();
        properties.put("group.id", groupId);
        properties.put("max.poll.records", this.messageNum);
        consumer = new KafkaConsumer<>(properties);
        subscribeProtocol(topic);
        return messageNum;
    }


    public void subscribeProtocol(String topic) {
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public List<String> getMessage() {
        List<String> message = new ArrayList<>();
        int count = 0;
        //控制拉取次数，若maxTimes次还没有拉满，则不拉了
        int times = 0;
        int maxTimes = 10;
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        while (count < messageNum) {
            ConsumerRecords<String, String> records = ConsumerAssistant.getMessage(consumer);
            for (ConsumerRecord<String, String> record : records) {
                message.add(record.topic() + "," + record.partition() + "," + record.offset() + "," + record.value());
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                count++;
                if (count == messageNum) {
                    break;
                }
            }
            times++;
            if (times == maxTimes) {
                break;
            }
        }
        ConsumerAssistant.commitSyncOffset(consumer, currentOffsets);
        return message;
    }*/
}
