package org.nit.kafkaUtil.util.readConfig;

import org.junit.Test;
import org.nit.kafkaUtil.model.TopicConfig;

import java.util.List;

/**
 * Created by kafkaTeam on 2018/5/9.
 */
public class ReadTopicConfigTest {

    @Test
    public void testGetInstance() throws Exception {
        List<TopicConfig> topicConfigs = ReadTopicConfig.getInstance("./conf/config-topic.xml").getTopicConfigsList();
    }
}