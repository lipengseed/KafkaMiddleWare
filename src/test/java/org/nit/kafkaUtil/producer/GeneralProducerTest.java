package org.nit.kafkaUtil.producer;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by BestPig on 2018/5/17.
 */
public class GeneralProducerTest {

    @Before
    public void setUp() throws Exception {
        PropertyConfigurator.configure("./conf/log4j.properties");
    }

    @Test
    public void testSendMessage() throws Exception {
        GeneralProducer dataQualityProducer = new GeneralProducer("./conf/kafka-generalProducer.properties","./conf/config-topic.xml");
        dataQualityProducer.configure();
        dataQualityProducer.manageTopic();

        for(int i=0;i<2000;i++) {
            dataQualityProducer.sendMessage("data1", "generalProducerData1  " + i);
            dataQualityProducer.sendMessage("data2", "generalProducerData2  " + i);
            //Thread.sleep(1000);
        }
        Thread.sleep(300*1000);
    }
}