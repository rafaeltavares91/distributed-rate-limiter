package io.github.rafaeltavares.ratelimiter.internal;

import io.github.rafaeltavares.ratelimiter.RateLimiterConfig;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.util.Objects;

/**
 * Manages local time windows and local request counting for a given rate limit key.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class LocalWindowManager {

    private final RateLimiterConfig config;

    public static LocalWindowManager of(RateLimiterConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        return new LocalWindowManager(config);
    }

    public long incrementAndGetRequestsInCurrentWindow(LocalCounterState state, long nowMillis) {
        moveToNextWindowIfNeeded(state, nowMillis);
        return state.incrementRequestsInCurrentWindow();
    }

    public long computeEffectiveLocalLimit(int limit) {
        return (long) limit + config.getBatchSize();
    }

    private void moveToNextWindowIfNeeded(LocalCounterState state, long nowMillis) {
        long windowDurationMillis = config.getWindowSeconds() * 1000L;

        while (true) {
            long currentWindowStartedAt = state.getCurrentWindowStartedAtMillis();

            if (nowMillis - currentWindowStartedAt < windowDurationMillis) {
                return;
            }

            if (state.tryMoveToNextWindow(currentWindowStartedAt, nowMillis)) {
                state.resetRequestsInCurrentWindow();
                return;
            }
        }
    }
}