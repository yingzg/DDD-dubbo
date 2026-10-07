package com.ddd.dubbo.domain.statemachine;

/** 状态机构建器工厂 */
public final class StateMachineBuilderFactory {

    private StateMachineBuilderFactory() {
    }

    public static <S, E, C> StateMachineBuilder<S, E, C> create() {
        return new StateMachineBuilder<S, E, C>();
    }
}
