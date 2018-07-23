package org.nit.kafkaUtil.cousumer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.nit.kafkaUtil.model.DataQualityMessage;

import java.io.IOException;
import java.util.Map;

/**
 *
 * @author KafkaTeam
 * @date 2018/5/22
 */
public class JavaBeanDeserializer implements Deserializer {
    private String encoding = "UTF8";
    private ObjectMapper objectMapper;
    protected static Logger logger = LogManager.getLogger(JavaBeanDeserializer.class);

    @Override
    public void configure(Map map, boolean b) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public DataQualityMessage deserialize(String s, byte[] bytes) {
        DataQualityMessage dataQualityMessage = null;
        try {
            dataQualityMessage = objectMapper.readValue(bytes, DataQualityMessage.class);
        } catch (IOException e) {
            logger.error("javaBean Deserializer is error" + e.toString(), e);
        }
        return dataQualityMessage;
    }

    @Override
    public void close() {

    }
}
