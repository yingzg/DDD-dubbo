package com.ddd.dubbo.app.config.filter;

import com.ddd.dubbo.domain.common.context.UserContext;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

/** Consumer 侧：把上下文写回 attachment 继续透传 */
@Activate(group = CommonConstants.CONSUMER)
public class DubboConsumerFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        UserContext ctx = UserContext.get();
        if (ctx != null) {
            if (ctx.getUserId() != null) {
                invocation.setAttachment("userId", String.valueOf(ctx.getUserId()));
            }
            invocation.setAttachment("locale", ctx.getLocale());
            invocation.setAttachment("timeZone", ctx.getTimeZone());
            invocation.setAttachment("tenantId", ctx.getTenantId());
        }
        return invoker.invoke(invocation);
    }
}
