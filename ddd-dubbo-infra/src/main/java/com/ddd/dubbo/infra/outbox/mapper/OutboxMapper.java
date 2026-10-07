package com.ddd.dubbo.infra.outbox.mapper;

import com.ddd.dubbo.infra.outbox.dataobject.OutboxDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface OutboxMapper {
    int insert(OutboxDO outboxDO);
    List<OutboxDO> selectPending(@Param("limit") int limit);
    int markSent(@Param("id") Long id);
    int increaseRetry(@Param("id") Long id);
}
