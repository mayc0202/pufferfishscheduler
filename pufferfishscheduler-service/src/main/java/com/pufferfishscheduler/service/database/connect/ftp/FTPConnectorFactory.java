package com.pufferfishscheduler.service.database.connect.ftp;

import com.pufferfishscheduler.common.constants.Constants;
import com.pufferfishscheduler.common.exception.BusinessException;
import com.pufferfishscheduler.service.database.connect.ftp.impl.FTPConnector;
import com.pufferfishscheduler.service.database.connect.ftp.impl.FTPSConnector;

/**
 * FTP连接工厂类
 */
public class FTPConnectorFactory {

    public static AbstractFTPConnector getConnector(String type) {
        switch (type) {
            case Constants.FTP_TYPE.FTP:
                return new FTPConnector();
            case Constants.FTP_TYPE.FTPS:
                return new FTPSConnector();
            default:
                throw new BusinessException(String.format("暂不支持此文件服务类型![type:%s]",type));
        }
    }
}
