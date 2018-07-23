package org.nit.kafkaUtil.cousumer;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.nit.kafkaUtil.producer.GeneralProducer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DataQualityConsumerTest {
    @Before
    public void setUp() throws Exception {
        PropertyConfigurator.configure("./conf/log4j.properties");
    }
    @Test
    public void testGetMessage(){
        List<String> topicList = new ArrayList<>();
        topicList.add("topicConsumerTest1");
        //topicList.add("topicConsumerTest2");
        List<String> message = new ArrayList<>();
        GeneralConsumer generalConsumer = new GeneralConsumer("./conf/kafka-dataQualityConsumer.properties","./conf/config-topic.xml");
        generalConsumer.configure("test2",topicList);
        int j=0;
        while(j<10) {
            message.clear();
            message = generalConsumer.getMessage(20,10);
            System.out.println(message.size());
            if (!message.isEmpty()){
                System.out.println(message.get(0));
                System.out.println(message.get(message.size()-1));
            }
            j++;
            generalConsumer.commitOffset();
        }
    }
}