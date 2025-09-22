package com.pufferfishscheduler.service.database.db.connector.ftp.impl;


import com.pufferfishscheduler.common.result.ConResponse;
import com.pufferfishscheduler.service.database.db.connector.ftp.AbstractFTPConnector;
import com.pufferfishscheduler.service.database.db.connector.ftp.FTPSManager;

public class FTPSConnector extends AbstractFTPConnector {

    @Override
    public ConResponse connect() {
        ConResponse testResponse = new ConResponse();
        FTPSManager ftpManager = new FTPSManager(this.getHost(),
                this.getPort(),
                this.getUsername(),
                this.getPassword(),
                this.getControlEncoding(),
                this.getMode());
        try {
            ftpManager.getFTPClient();
            testResponse.setResult(true);
            testResponse.setMsg("连接成功！");
        } catch (Exception e) {
            testResponse.setResult(false);
            testResponse.setMsg(e.getMessage());
        } finally {
            // 关闭连接
            ftpManager.close();
        }
        return testResponse;
    }
}
