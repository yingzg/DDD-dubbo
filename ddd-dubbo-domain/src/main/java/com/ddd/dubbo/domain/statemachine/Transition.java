package com.ddd.dubbo.domain.statemachine;

import java.util.function.Consumer;
import java.util.function.Predicate;

/** 状态转移定义 */
public class Transition<S, E, C> {
    private final S from;
    private final S to;
    private final E event;
    private Predicate<C> condition;
    private Consumer<C> action;

    Transition(S from, S to, E event) {
        this.from = from;
        this.to = to;
        this.event = event;
    }

    public S getFrom() { return from; }
    public S getTo() { return to; }
    public E getEvent() { return event; }
    public Predicate<C> getCondition() { return condition; }
    public void setCondition(Predicate<C> condition) { this.condition = condition; }
    public Consumer<C> getAction() { return action; }
    public void setAction(Consumer<C> action) { this.action = action; }
}
