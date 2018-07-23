package org.nit.kafkaUtil.model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * 系统级配置文件的管理
 * @author kafkaTeam
 * @date 2018/5/9
 */
public class SystemConfig {
    protected static Logger logger = LogManager.getLogger(SystemConfig.class);
    /**
     * 系统级配置文件的地址固定
     */
    private final String configSystemPath = "./conf/config-system.properties";
    /**
     * Zookeeper地址
     */
    private String zkConnectList;
    /**
     * Zookeeper会话超时时间
     */
    private int sessionTimeout;
    /**
     * Zookeeper连接超时时间
     */
    private int connectTimeout;
    /**
     *  consumer拉取得最大时间
     */
    private long pollTimeout;

    /**
     * 解析系统配置
     */
    public void parseConfigSystem(){
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(new File(configSystemPath));
            properties.load(inputStream);
            this.zkConnectList = properties.getProperty("zkConnectList");
            this.sessionTimeout = Integer.parseInt(properties.getProperty("sessionTimeout"));
            this.connectTimeout = Integer.parseInt(properties.getProperty("connectTimeout"));
            this.pollTimeout = Long.parseLong(properties.getProperty("pollTimeout"));
            inputStream.close();
        } catch (Exception e) {
            logger.error("Read System Config Is Error !" + e.toString(), e);
        }
    }

    public String getZkConnectList() {
        return zkConnectList;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }
}
