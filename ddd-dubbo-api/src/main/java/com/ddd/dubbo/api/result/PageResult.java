package com.ddd.dubbo.api.result;

import java.io.Serializable;
import java.util.List;

/** 统一分页响应 */
public class PageResult<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private long total;
    private List<T> list;

    public static <T> PageResult<T> of(long total, List<T> list) {
        PageResult<T> p = new PageResult<T>();
        p.total = total;
        p.list = list;
        return p;
    }

    public long getTotal() { return total; }
    public void setTotal(long total) { this.total = total; }
    public List<T> getList() { return list; }
    public void setList(List<T> list) { this.list = list; }
}
