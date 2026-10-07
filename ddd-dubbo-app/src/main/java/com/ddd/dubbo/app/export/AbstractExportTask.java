package com.ddd.dubbo.app.export;

import java.io.File;

/** 异步导出任务基类：分页查询 + 流式写文件 + 上传，防止 OOM */
public abstract class AbstractExportTask<T> {

    private final ObjectStorageWriter objectStorageWriter;

    protected AbstractExportTask(ObjectStorageWriter objectStorageWriter) {
        this.objectStorageWriter = objectStorageWriter;
    }

    public String execute(ExportRequest request) {
        File file = streamWrite(request);
        return objectStorageWriter.upload(request.getBizType(), file);
    }

    /** 分页查询 + 流式写文件（子类实现，避免一次性加载全部数据） */
    protected abstract File streamWrite(ExportRequest request);
}
