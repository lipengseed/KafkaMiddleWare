package org.nit.kafkaUtil.util.topicManage;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.nit.kafkaUtil.model.SystemConfig;

import static org.junit.Assert.*;

public class TopicAssistantTest {
    @Before
    public void setUp() throws Exception {
        PropertyConfigurator.configure("./conf/log4j.properties");
    }
    @Test
    public void testCreateTopic(){
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.parseConfigSystem();
        TopicAssistant.createTopic(systemConfig, "./conf/config-topic.xml");
    }

}