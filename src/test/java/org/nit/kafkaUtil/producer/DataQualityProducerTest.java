package org.nit.kafkaUtil.producer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.nit.kafkaUtil.model.DataQualityMessage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kafkaTeam on 2018/5/10.
 */
public class DataQualityProducerTest {

    @Before
    public void setUp() throws Exception {
        PropertyConfigurator.configure("./conf/log4j.properties");
    }

    public static void main(String[] args){
        PropertyConfigurator.configure("./conf/log4j.properties");
        int producerThreadNum = 4;
        ExecutorService executor = Executors.newFixedThreadPool(producerThreadNum);
        DataQualityProducer dataQualityProducer1 = new DataQualityProducer("./conf/kafka-dataQualityProducer.properties","./conf/config-topic.xml");
        DataQualityProducer dataQualityProducer2 = new DataQualityProducer("./conf/kafka-dataQualityProducer.properties","./conf/config-topic.xml");
        dataQualityProducer1.configure();
        dataQualityProducer1.manageTopic();
        dataQualityProducer2.configure();
        dataQualityProducer2.manageTopic();
        int count = 0;
        while(count<1000000){
            int i = (int) (Math.random()*10%2);
            String key = "data5";
            DataQualityMessage value = new DataQualityMessage();
            if(i==0) {
                executor.submit(new SendMessageThreadTest(dataQualityProducer1,key,value));
            }else{
                executor.submit(new SendMessageThreadTest(dataQualityProducer2,key,value));
            }
            count++;
        }
    }

    @Test
    public void testSendMessage() throws Exception {
        DataQualityProducer dataQualityProducer = new DataQualityProducer("./conf/kafka-dataQualityProducer.properties","./conf/config-topic.xml");
        dataQualityProducer.configure();
        dataQualityProducer.manageTopic();
        DataQualityMessage dataQualityMessage = new DataQualityMessage();
        dataQualityMessage.setDataRecord("test test test");
        dataQualityMessage.setProtocolName("data5");
        dataQualityMessage.setZipFileName("****.zip");
        dataQualityMessage.setHostName("192.168.1.152");
        for(int i=0;i<20;i++) {
            dataQualityProducer.sendMessage("data5", dataQualityMessage);
        }
        Thread.sleep(300*1000);
    }
}