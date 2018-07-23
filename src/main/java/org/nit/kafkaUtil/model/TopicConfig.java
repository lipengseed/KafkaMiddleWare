package org.nit.kafkaUtil.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author kafkaTeam
 * @date 2018/5/9
 */
public class TopicConfig {

    /**
     * topic的名称
     */
    private String topicName;

    /**
     * topic的复制因子
     */
    private Integer repilcaNum;

    /**
     * 该topic日志最大保留大小
     */
    private String retentionBytes;

    /**
     * 该topic日志最长保留时间
     */
    private String retentionMs;

    /**
     * 数据类型和对应partition的映射
     */
    private Map<String, List<Integer>> protocolPartion = new HashMap<>();

    /**
     * topic对应的patition数量
     */
    private Integer topicPartitionNum;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getRepilcaNum() {
        return repilcaNum;
    }

    public void setRepilcaNum(Integer repilcaNum) {
        this.repilcaNum = repilcaNum;
    }

    public Map<String, List<Integer>> getProtocolPartion() {
        return protocolPartion;
    }

    public void setProtocolPartion(Map<String, List<Integer>> protocolPartion) {
        this.protocolPartion = protocolPartion;
    }

    public Integer getTopicPartitionNum() {
        return topicPartitionNum;
    }

    public void setTopicPartitionNum(Integer topicPartitionNum) {
        this.topicPartitionNum = topicPartitionNum;
    }

    public String getRetentionBytes() {
        return retentionBytes;
    }

    public void setRetentionBytes(String retentionBytes) {
        this.retentionBytes = retentionBytes;
    }

    public String getRetentionMs() {
        return retentionMs;
    }

    public void setRetentionMs(String retentionMs) {
        this.retentionMs = retentionMs;
    }

    public Properties propertiesConfig(){
        Properties properties = new Properties();
        properties.put("retention.bytes",this.retentionBytes);
        properties.put("retention.ms",this.retentionMs);
        return properties;
    }
}
