package org.nit.kafkaUtil.model;

/**
 *
 * @author kafkaTeam
 * @date 2018/5/7
 */
public class DataQualityMessage {
    /**
     * 数据所属的数据类型
     */
    private String protocolName;

    /**
     * zip包所属的名称
     */
    private String zipFileName;

    /**
     * zip包所在的机器Ip
     */
    private String hostName;

    /**
     * 单条的数据记录
     */
    private String dataRecord;

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }

    public String getZipFileName() {
        return zipFileName;
    }

    public void setZipFileName(String zipFileName) {
        this.zipFileName = zipFileName;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getDataRecord() {
        return dataRecord;
    }

    public void setDataRecord(String dataRecord) {
        this.dataRecord = dataRecord;
    }
}
