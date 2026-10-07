package com.ddd.dubbo.domain.statemachine;

import com.ddd.dubbo.domain.common.exception.BizException;

import java.util.HashMap;
import java.util.Map;

/** 无状态状态机：不保存当前状态，每次 fireEvent 传入 from 返回 to */
public class StateMachine<S, E, C> {
    private static final int STATE_TRANSITION_ERROR = 5001;
    private static final int STATE_CONDITION_ERROR = 5002;

    private final Map<S, Map<E, Transition<S, E, C>>> transitions;

    StateMachine(Map<S, Map<E, Transition<S, E, C>>> transitions) {
        this.transitions = transitions;
    }

    public S fireEvent(S from, E event, C context) {
        Map<E, Transition<S, E, C>> map = transitions.get(from);
        if (map == null || !map.containsKey(event)) {
            throw new BizException(STATE_TRANSITION_ERROR, "非法状态流转: " + from + " -> " + event);
        }
        Transition<S, E, C> t = map.get(event);
        if (t.getCondition() != null && !t.getCondition().test(context)) {
            throw new BizException(STATE_CONDITION_ERROR, "状态流转条件不满足");
        }
        if (t.getAction() != null) {
            t.getAction().accept(context);
        }
        return t.getTo();
    }
}
