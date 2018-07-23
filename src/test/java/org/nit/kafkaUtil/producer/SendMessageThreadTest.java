package org.nit.kafkaUtil.producer;

import org.nit.kafkaUtil.model.DataQualityMessage;

public class SendMessageThreadTest implements Runnable{
    private DataQualityProducer dataQualityProducer;
    private String key;
    private DataQualityMessage value;
    public SendMessageThreadTest(DataQualityProducer dataQualityProducer,String key,DataQualityMessage value){
        this.dataQualityProducer = dataQualityProducer;
        this.key = key;
        this.value = value;
    }
    @Override
    public void run(){
        dataQualityProducer.sendMessage(key,value);
    }
}
