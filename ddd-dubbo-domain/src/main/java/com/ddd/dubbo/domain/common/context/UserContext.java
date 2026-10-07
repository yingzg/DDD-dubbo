package com.ddd.dubbo.domain.common.context;

/** 用户上下文（不可变，ThreadLocal 持有，入口恢复 finally 清理） */
public final class UserContext {

    private static final ThreadLocal<UserContext> HOLDER = new ThreadLocal<UserContext>();

    private final Long userId;
    private final String locale;
    private final String timeZone;
    private final String tenantId;

    private UserContext(Builder builder) {
        this.userId = builder.userId;
        this.locale = builder.locale;
        this.timeZone = builder.timeZone;
        this.tenantId = builder.tenantId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UserContext get() { return HOLDER.get(); }
    public static void set(UserContext ctx) { HOLDER.set(ctx); }
    public static void clear() { HOLDER.remove(); }

    public Long getUserId() { return userId; }
    public String getLocale() { return locale; }
    public String getTimeZone() { return timeZone; }
    public String getTenantId() { return tenantId; }

    public static final class Builder {
        private Long userId;
        private String locale;
        private String timeZone;
        private String tenantId;

        public Builder userId(Long userId) { this.userId = userId; return this; }
        public Builder locale(String locale) { this.locale = locale; return this; }
        public Builder timeZone(String timeZone) { this.timeZone = timeZone; return this; }
        public Builder tenantId(String tenantId) { this.tenantId = tenantId; return this; }

        public UserContext build() {
            return new UserContext(this);
        }
    }
}
