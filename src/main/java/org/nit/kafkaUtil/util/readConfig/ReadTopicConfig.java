package org.nit.kafkaUtil.util.readConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.nit.kafkaUtil.model.TopicConfig;

import java.io.File;
import java.util.*;

/**
 * 采用单例模式读取配置文件
 * @author kafkaTeam
 * @date 2018/5/9
 */
public class ReadTopicConfig {
    protected static Logger logger = LogManager.getLogger(ReadTopicConfig.class);
    /**
     * 考虑到后续可能频繁使用，在内存中保存一份
     */
    private final static ReadTopicConfig instance = new ReadTopicConfig();

    private static boolean configIsRead = false;

    private List<TopicConfig> topicConfigsList = new LinkedList<>();

    public static synchronized ReadTopicConfig getInstance(String configPath){
        if (configIsRead){
            return instance;
        }else {
            instance.loadTopicConfig(configPath);
            configIsRead = true;
            return instance;
        }
    }


    /**
     * 解析具体topic配置文件
     * @param configPath
     */
    private void loadTopicConfig(String configPath){
        //清空List内容
        topicConfigsList.clear();

        SAXReader reader = new SAXReader();

        try {
            Document document = reader.read(new File(configPath));
            List<Element> topicsNodes = document.getRootElement().element("topics").elements("topic");

            for (Element topicsNode : topicsNodes){
                TopicConfig topicConfig = new TopicConfig();
                topicConfig.setTopicName(topicsNode.attribute("topic_name").getValue());
                topicConfig.setRepilcaNum(Integer.parseInt(topicsNode.attribute("repilca_num").getValue()));
                topicConfig.setRetentionBytes(topicsNode.attribute("retention_bytes").getValue());
                topicConfig.setRetentionMs(topicsNode.attribute("retention_ms").getValue());

                //解析protocol节点部分
                List<Element> protocolNodes = topicsNode.elements("protocol");
                Map<String, List<Integer>> protocolPartition = new HashMap<>();
                Integer topicPartitionNum = 0;
                for (Element protocolNode : protocolNodes){
                    List<Integer> partitionNum = StringToList(protocolNode.attribute("partition").getValue());
                    topicPartitionNum += partitionNum.size();
                    protocolPartition.put(protocolNode.attribute("protocol_name").getValue(), partitionNum);
                }
                topicConfig.setProtocolPartion(protocolPartition);
                topicConfig.setTopicPartitionNum(topicPartitionNum);

                //将topicConfig对象塞入List中
                this.topicConfigsList.add(topicConfig);
            }

        } catch (Exception e) {
            logger.error("Read TopicConfig Is Error !" + e.toString(), e);
        }

    }

    /**
     * 将0-1-2 形式的数据转为List形式
     * @param partitionSet
     * @return
     */
    public List<Integer> StringToList(String partitionSet) throws NumberFormatException{
        List<Integer> list = new LinkedList<>();
        String[] partitions = partitionSet.split("-");
        for (String partition: partitions){
            list.add(Integer.parseInt(partition));
        }
        return list;
    }

    /**
     * 向外部提供topic的集合
     * @return
     */
    public List<TopicConfig> getTopicConfigsList() {
        return topicConfigsList;
    }

}
