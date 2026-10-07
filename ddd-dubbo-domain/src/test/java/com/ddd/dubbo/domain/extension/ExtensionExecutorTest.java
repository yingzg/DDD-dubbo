package com.ddd.dubbo.domain.extension;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExtensionExecutorTest {

    interface DiscountExtPt extends ExtensionPoint {
        String discount();
    }

    @Extension(bizId = "normal")
    static class NormalExt implements DiscountExtPt {
        @Override
        public String discount() { return "normal"; }
    }

    @Extension(bizId = "vip")
    static class VipExt implements DiscountExtPt {
        @Override
        public String discount() { return "vip"; }
    }

    @Test
    void routesByBizId() {
        List<DiscountExtPt> exts = Arrays.<DiscountExtPt>asList(new NormalExt(), new VipExt());
        ExtensionExecutor executor = new ExtensionExecutor();

        assertEquals("vip", executor.match(exts, DiscountExtPt.class,
                BizScenario.of("vip", "order", "default")).discount());
        assertEquals("normal", executor.match(exts, DiscountExtPt.class,
                BizScenario.of("normal", "order", "default")).discount());
    }

    @Test
    void throwsWhenNoExtension() {
        ExtensionExecutor executor = new ExtensionExecutor();
        assertThrows(BizException.class, () ->
                executor.match(Arrays.<DiscountExtPt>asList(),
                        DiscountExtPt.class, BizScenario.of("vip", "order", "default")));
    }
}
