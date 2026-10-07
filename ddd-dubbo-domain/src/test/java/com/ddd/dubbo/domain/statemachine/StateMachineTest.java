package com.ddd.dubbo.domain.statemachine;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StateMachineTest {

    enum S { INIT, PAID }
    enum E { PAY }

    @Test
    void firesTransitionAndReturnsTarget() {
        StateMachineBuilder<S, E, String> builder = StateMachineBuilderFactory.create();
        builder.transition().from(S.INIT).to(S.PAID).on(E.PAY).build();
        StateMachine<S, E, String> sm = builder.build();

        assertEquals(S.PAID, sm.fireEvent(S.INIT, E.PAY, "ctx"));
    }

    @Test
    void throwsOnIllegalTransition() {
        StateMachineBuilder<S, E, String> builder = StateMachineBuilderFactory.create();
        builder.transition().from(S.INIT).to(S.PAID).on(E.PAY).build();
        StateMachine<S, E, String> sm = builder.build();

        assertThrows(BizException.class, () -> sm.fireEvent(S.PAID, E.PAY, "ctx"));
    }

    @Test
    void conditionRejectsTransition() {
        StateMachineBuilder<S, E, Integer> builder = StateMachineBuilderFactory.create();
        builder.transition().from(S.INIT).to(S.PAID).on(E.PAY)
                .when(ctx -> ctx > 100).build();
        StateMachine<S, E, Integer> sm = builder.build();

        assertThrows(BizException.class, () -> sm.fireEvent(S.INIT, E.PAY, 10));
        assertEquals(S.PAID, sm.fireEvent(S.INIT, E.PAY, 200));
    }
}
