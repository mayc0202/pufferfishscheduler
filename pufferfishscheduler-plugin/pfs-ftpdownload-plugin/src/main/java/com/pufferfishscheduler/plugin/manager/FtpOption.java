package com.pufferfishscheduler.plugin.manager;

/**
 * @Author: yc
 * @CreateTime: 2024-12-09
 * @Description: FTP配置参数
 * @Version: 1.0
 */
public class FtpOption {

    private String host;
    private Integer port;
    private String username;
    private String password;
    private Integer timeout;
    private Boolean binaryMode;
    private String controlEncoding;
    private String mode;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Boolean getBinaryMode() {
        return binaryMode;
    }

    public void setBinaryMode(Boolean binaryMode) {
        this.binaryMode = binaryMode;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public void setControlEncoding(String controlEncoding) {
        this.controlEncoding = controlEncoding;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}