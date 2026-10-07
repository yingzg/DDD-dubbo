package com.ddd.dubbo.domain.extension;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 扩展点执行器：按 BizScenario 三级降级路由 */
@Component
public class ExtensionExecutor {

    private static final int SYS_ERROR = 5000;

    private final ApplicationContext applicationContext;

    public ExtensionExecutor() {
        this.applicationContext = null; // 单测用（match 不依赖容器）
    }

    @Autowired
    public ExtensionExecutor(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public <T extends ExtensionPoint> T execute(Class<T> extPtClass, BizScenario scenario) {
        Map<String, T> exts = applicationContext.getBeansOfType(extPtClass);
        return match(new ArrayList<T>(exts.values()), extPtClass, scenario);
    }

    /** 三级降级匹配：bizId+useCase+scenario → bizId+useCase → bizId */
    public <T extends ExtensionPoint> T match(List<T> exts, Class<T> extPtClass, BizScenario scenario) {
        T best = null;
        int bestScore = -1;
        for (T ext : exts) {
            Extension ann = ClassUtils.getUserClass(ext.getClass()).getAnnotation(Extension.class);
            if (ann == null) {
                continue;
            }
            int score = matchScore(ann, scenario);
            if (score > bestScore) {
                bestScore = score;
                best = ext;
            }
        }
        if (best == null) {
            throw new BizException(SYS_ERROR,
                    "缺少扩展实现: " + extPtClass.getSimpleName() + " for " + scenario.getBizId());
        }
        return best;
    }

    private int matchScore(Extension ann, BizScenario scenario) {
        if (!ann.bizId().equals(scenario.getBizId())) {
            return -1;
        }
        if (ann.useCase().equals(scenario.getUseCase()) && ann.scenario().equals(scenario.getScenario())) {
            return 3;
        }
        if (ann.useCase().equals(scenario.getUseCase())) {
            return 2;
        }
        return 1;
    }
}
