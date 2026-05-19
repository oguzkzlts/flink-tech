package org.example.flink.models;

import java.io.Serializable;

public class DeadLetterEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String originalPayload;
    private String errorMessage;
    private String errorType;
    private long timestamp;
    private String source;
    private int retryCount;
    private String stackTrace;

    public DeadLetterEvent() {
        this.timestamp = System.currentTimeMillis();
        this.retryCount = 0;
    }

    public String getOriginalPayload() { return originalPayload; }
    public void setOriginalPayload(String originalPayload) { this.originalPayload = originalPayload; }
    
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    
    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    
    public String getStackTrace() { return stackTrace; }
    public void setStackTrace(String stackTrace) { this.stackTrace = stackTrace; }

    @Override
    public String toString() {
        return String.format("DLQ | ERROR:%s | TYPE:%s | PAYLOAD:%s | TS:%d", 
                errorMessage, errorType, originalPayload, timestamp);
    }
}
