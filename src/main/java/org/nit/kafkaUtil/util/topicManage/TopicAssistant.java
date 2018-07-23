package org.nit.kafkaUtil.util.topicManage;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.nit.kafkaUtil.model.SystemConfig;
import org.nit.kafkaUtil.model.TopicConfig;
import org.nit.kafkaUtil.util.readConfig.ReadTopicConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 对kafka中间的topic进行管理，包括增加、删除、修改等
 * @author kafkaTeam
 * @date 2018/5/9
 */
public class TopicAssistant {
    protected static Logger logger = LogManager.getLogger(TopicAssistant.class);


    public static boolean createTopic(SystemConfig systemConfig, String topicConfigPath){
        List<TopicConfig> topicConfigs = ReadTopicConfig.getInstance(topicConfigPath).getTopicConfigsList();
        ZkUtils zkUtils = null;
        try{
            zkUtils = ZkUtils.apply(systemConfig.getZkConnectList(), systemConfig.getSessionTimeout(), systemConfig.getConnectTimeout(), JaasUtils.isZkSecurityEnabled());
            for (TopicConfig topicConfig : topicConfigs){
                if(!AdminUtils.topicExists(zkUtils, topicConfig.getTopicName())){
                    AdminUtils.createTopic(zkUtils, topicConfig.getTopicName(), topicConfig.getTopicPartitionNum(), topicConfig.getRepilcaNum(), topicConfig.propertiesConfig(), AdminUtils.createTopic$default$6());
                    logger.info(topicConfig.getTopicName() + " creates sucessfully");
                }else {
                    logger.debug(topicConfig.getTopicName() + " already exits");
                }
            }

        }catch(Exception e){
            logger.error("topic created failed" + e.toString(), e);
            return false;
        }finally{
            if(zkUtils!=null) {
                zkUtils.close();
            }
        }
        return true;
    }

    /**
     * 通过协议的名称寻找对应的topic名称
     * @param topicConfigList
     * @param protocolName
     * @return
     */
    public static String getTopicName(List<TopicConfig> topicConfigList, String protocolName){
        String topicName = "";
        for (TopicConfig topicConfig : topicConfigList){
            Map<String, List<Integer>> protocolPartition = topicConfig.getProtocolPartion();
            for (Map.Entry<String, List<Integer>> entry : protocolPartition.entrySet()){
                if (protocolName.equalsIgnoreCase(entry.getKey())){
                    topicName = topicConfig.getTopicName();
                }
            }
        }
        return topicName;
    }

    /**
     * 根据给定的主题名获取该主题的分区数量
     * @param topicConfigList
     * @param topicName
     * @return
     */
    public static int getTopicPartitionNum(List<TopicConfig> topicConfigList, String topicName){
        int num = -1;
        for (TopicConfig topicConfig : topicConfigList){
            String name = topicConfig.getTopicName();
            if(name.equals(topicName)){
                num = topicConfig.getTopicPartitionNum();
            }
        }
        return num;
    }
}
