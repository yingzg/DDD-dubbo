package com.ddd.dubbo.app.export;

public class ExportRequest {
    private String taskId;
    private String bizType;

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getBizType() { return bizType; }
    public void setBizType(String bizType) { this.bizType = bizType; }
}
