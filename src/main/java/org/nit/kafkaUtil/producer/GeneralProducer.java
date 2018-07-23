package org.nit.kafkaUtil.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.nit.kafkaUtil.model.DataQualityMessage;
import org.nit.kafkaUtil.model.ProducerConfig;
import org.nit.kafkaUtil.model.SystemConfig;
import org.nit.kafkaUtil.model.TopicConfig;
import org.nit.kafkaUtil.util.readConfig.ReadTopicConfig;
import org.nit.kafkaUtil.util.topicManage.TopicAssistant;

import java.util.List;
import java.util.Properties;

/**
 * 普通的生产者，直接向外提供字符串
 * @author Kafka Team
 * @date 2018/5/17
 */
public class GeneralProducer extends BaseProducer<String, String> {
    protected static Logger logger = LogManager.getLogger(DataQualityProducer.class);

    public GeneralProducer(String producerPropsPath, String configTopicPath){
        this.producerPropsPath = producerPropsPath;
        this.configTopicPath = configTopicPath;
    }

    @Override
    public void configure() {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.parseProducerConfig(producerPropsPath);
        Properties properties = producerConfig.propertiesConfig();
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void manageTopic() {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.parseConfigSystem();
        TopicAssistant.createTopic(systemConfig, configTopicPath);
    }

    @Override
    public boolean sendMessage(String protocolName, String value) {
        ProducerRecord<String, Object> record = null;
        List<TopicConfig> topicConfigList = ReadTopicConfig.getInstance(configTopicPath).getTopicConfigsList();
        String topic = TopicAssistant.getTopicName(topicConfigList, protocolName);
        if ("".equals(topic)){
            logger.error("Send Message is Error ! Not Found Topic Name");
            return false;
        }
        Integer partition = ProducerAssistant.getPartitionKey(topicConfigList, protocolName);
        if (partition == -1){
            logger.error("Send Message is Error ! Not Found Partition Id");
            return false;
        }

        record = new ProducerRecord<>(topic, partition, null, value);

        ProducerAssistant.sendMessage(producer, record);

        return true;
    }
}
