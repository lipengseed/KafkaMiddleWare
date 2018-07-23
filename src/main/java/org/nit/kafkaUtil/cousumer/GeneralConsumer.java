package org.nit.kafkaUtil.cousumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.nit.kafkaUtil.model.ConsumerConfig;
import org.nit.kafkaUtil.model.SystemConfig;
import org.nit.kafkaUtil.model.TopicConfig;
import org.nit.kafkaUtil.util.readConfig.ReadTopicConfig;

import java.util.*;

/**
 *
 * @author KafkaTeam
 * @date 2018/5/23
 */
public class GeneralConsumer extends BaseConsumer<String, String>{

    protected static Logger logger = LogManager.getLogger(GeneralConsumer.class);

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public GeneralConsumer(String consumerPropsPath, String configTopicPath) {
        this.consumerPropsPath = consumerPropsPath;
        this.configTopicPath = configTopicPath;
        this.messageNum = 1000;
    }

    @Override
    public int configure(String groupId, List<String> topicList) {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.parseConfigSystem();
        this.pollTimeout = systemConfig.getPollTimeout();

        List<TopicConfig> topicConfigList = ReadTopicConfig.getInstance(configTopicPath).getTopicConfigsList();
        if (!ConsumerAssistant.isExistTopic(topicConfigList, topicList)){
            logger.error("Send Message is Error ! Not Found Topic Name");
        }

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.parseConsumerConfig(consumerPropsPath);
        Properties properties = consumerConfig.propertiesConfig();
        properties.put("group.id", groupId);
        properties.put("max.poll.records", this.messageNum);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topicList, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                ConsumerAssistant.commitSyncOffset(consumer, currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                currentOffsets.clear();
            }
        });

        return messageNum;
    }

    @Override
    public List<String> getMessage(int minBatchSize, long timeout) {
        List<String> message = new ArrayList<>();
        ConsumerRecords<String, String> records;
        while(timeout > 0){
            if (timeout < pollTimeout){
                records = ConsumerAssistant.getMessage(consumer, timeout);
            }else {
                records = ConsumerAssistant.getMessage(consumer, pollTimeout);
            }

            for (TopicPartition partition : records.partitions()){
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords){
                    message.add(record.topic() + "," + record.partition() + "," + record.offset() + "," + record.value());
                }

                long lastOffset = partitionRecords.get(partitionRecords.size() -1).offset();

                synchronized (currentOffsets) {
                    if (!currentOffsets.containsKey(partition)){
                        currentOffsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    } else {
                        long existOffset = currentOffsets.get(partition).offset();
                        if (existOffset <= lastOffset + 1) {
                            currentOffsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                        }
                    }
                }

            }

            if (message.size() >= minBatchSize){
                break;
            }

            timeout = timeout - pollTimeout;
        }

        return message;
    }

    @Override
    public void commitOffset() {
        //减少synchronized锁定currentOffset的时间
        Map<TopicPartition, OffsetAndMetadata> unmodifiedOffset;
        if (!currentOffsets.isEmpty() && consumer != null){
            synchronized (currentOffsets){
                if (currentOffsets.isEmpty()){
                    return;
                }
                unmodifiedOffset = Collections.unmodifiableMap(new HashMap<>(currentOffsets));
                currentOffsets.clear();
            }
            ConsumerAssistant.commitSyncOffset(consumer, unmodifiedOffset);
        }

    }


}
