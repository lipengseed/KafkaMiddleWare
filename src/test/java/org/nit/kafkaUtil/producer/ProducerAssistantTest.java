package org.nit.kafkaUtil.producer;

import org.junit.Before;
import org.junit.Test;
import org.nit.kafkaUtil.model.TopicConfig;
import org.nit.kafkaUtil.util.readConfig.ReadTopicConfig;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by BestPig on 2018/5/10.
 */
public class ProducerAssistantTest {

    private List<TopicConfig> topicConfigList;
    @Before
    public void setUp() throws Exception {
        topicConfigList = ReadTopicConfig.getInstance("./conf/config-topic.xml").getTopicConfigsList();
    }


    @Test
    public void testGetPartitionKey() throws Exception {
        assertEquals("-1",String.valueOf(ProducerAssistant.getPartitionKey(topicConfigList,"data11")));
        //System.out.println(ProducerAssistant.getPartitionKey(topicConfigList,"data2"));
    }
}