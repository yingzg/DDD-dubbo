package com.ddd.dubbo.domain.extension;

/** 业务场景：bizId + useCase + scenario 三级路由 */
public class BizScenario {
    public static final String DEFAULT = "default";

    private String bizId;
    private String useCase;
    private String scenario;

    public static BizScenario of(String bizId) {
        return of(bizId, DEFAULT, DEFAULT);
    }

    public static BizScenario of(String bizId, String useCase, String scenario) {
        BizScenario bs = new BizScenario();
        bs.bizId = bizId;
        bs.useCase = useCase;
        bs.scenario = scenario;
        return bs;
    }

    public String getBizId() { return bizId; }
    public String getUseCase() { return useCase; }
    public String getScenario() { return scenario; }
}
