package com.pufferfishscheduler.worker.task.trans.engine.exception;

/**
 * 数据转换引擎异常
 *
 * @author Mayc
 * @since 2026-03-04 17:44
 */
public class DataTransformationEngineException extends RuntimeException {

	private static final long serialVersionUID = -378754169581540153L;
	private String errorCode;
    private String message;

    public DataTransformationEngineException() {
    }

    public DataTransformationEngineException(String message) {
        this.message = message;
    }

    public DataTransformationEngineException(String errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }
    
    public DataTransformationEngineException(String message, Throwable cause) {
    	super(message, cause);
    	
    	this.message=message;
    }

    public String getErrorCode() {
        return this.errorCode;
    }

    public String getMessage() {
        return this.message;
    }
}
