package org.nit.kafkaUtil.producer;


import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * 对自定义的类进行序列化
 * @author kafkaTeam
 * @date 2018/5/17
 */
public class javaBeanSerializer implements Serializer {
    protected static Logger logger = LogManager.getLogger(javaBeanSerializer.class);

    private ObjectMapper objectMapper;

    private String encoding = "UTF8";

    @Override
    public void configure(Map map, boolean b) {
        objectMapper = new ObjectMapper();

        /*String propertyName = b?"key.serializer.encoding":"value.serializer.encoding";
        Object encodingValue = map.get(propertyName);
        if(encodingValue == null) {
            encodingValue = map.get("serializer.encoding");
        }

        if(encodingValue != null && encodingValue instanceof String) {
            this.encoding = (String)encodingValue;
        }*/
    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(o).getBytes(encoding);
        } catch (IOException e) {
            logger.error("javaBean Serializer is error" + e.toString(),e);
        }
        return ret;
    }

    @Override
    public void close() {

    }
}
