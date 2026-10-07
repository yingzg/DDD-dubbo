package com.ddd.dubbo.domain.statemachine;

import java.util.HashMap;
import java.util.Map;

/** 状态机构建器 */
public class StateMachineBuilder<S, E, C> {

    private final Map<S, Map<E, Transition<S, E, C>>> transitions = new HashMap<S, Map<E, Transition<S, E, C>>>();

    public TransitionBuilder transition() {
        return new TransitionBuilder();
    }

    public StateMachine<S, E, C> build() {
        return new StateMachine<S, E, C>(transitions);
    }

    public class TransitionBuilder {
        private S from;
        private S to;
        private E event;
        private java.util.function.Predicate<C> condition;
        private java.util.function.Consumer<C> action;

        public TransitionBuilder from(S from) { this.from = from; return this; }
        public TransitionBuilder to(S to) { this.to = to; return this; }
        public TransitionBuilder on(E event) { this.event = event; return this; }

        public TransitionBuilder when(java.util.function.Predicate<C> condition) {
            this.condition = condition;
            return this;
        }

        public TransitionBuilder perform(java.util.function.Consumer<C> action) {
            this.action = action;
            return this;
        }

        public StateMachineBuilder<S, E, C> build() {
            Transition<S, E, C> t = new Transition<S, E, C>(from, to, event);
            t.setCondition(condition);
            t.setAction(action);
            Map<E, Transition<S, E, C>> map = transitions.get(from);
            if (map == null) {
                map = new HashMap<E, Transition<S, E, C>>();
                transitions.put(from, map);
            }
            map.put(event, t);
            return StateMachineBuilder.this;
        }
    }
}
