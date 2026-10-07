package com.ddd.dubbo.app.export;

import java.io.File;

/** 对象存储写入（导出文件服务端上传的最小能力） */
public interface ObjectStorageWriter {
    String upload(String directory, File file);
}
