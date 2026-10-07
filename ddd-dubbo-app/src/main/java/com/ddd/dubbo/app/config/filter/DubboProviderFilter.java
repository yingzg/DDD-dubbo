package com.ddd.dubbo.app.config.filter;

import com.ddd.dubbo.domain.common.context.UserContext;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

@Activate(group = CommonConstants.PROVIDER)
public class DubboProviderFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            String userId = invocation.getAttachment("userId");
            Long uid = userId == null ? null : Long.valueOf(userId);
            UserContext ctx = UserContext.builder()
                    .userId(uid)
                    .locale(invocation.getAttachment("locale"))
                    .timeZone(invocation.getAttachment("timeZone"))
                    .tenantId(invocation.getAttachment("tenantId"))
                    .build();
            UserContext.set(ctx);
            return invoker.invoke(invocation);
        } finally {
            UserContext.clear();
        }
    }
}
