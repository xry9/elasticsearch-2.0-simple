package com.xry9.elasticsearch;

public class EsEnv {
    /**
     * 集群名
     */
    private String clusterName;

    /**
     * ip集合
     */
    private String ip;

    /**
     * port集合
     */
    private String port;

    public EsEnv(String clusterName, String ip, String port) {
        super();
        this.clusterName = clusterName;
        this.ip = ip;
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
