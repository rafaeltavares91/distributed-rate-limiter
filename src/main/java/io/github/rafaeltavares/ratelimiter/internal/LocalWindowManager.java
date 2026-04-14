package io.github.rafaeltavares.ratelimiter.internal;

import io.github.rafaeltavares.ratelimiter.RateLimiterConfig;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.util.Objects;

/**
 * Manages local time windows and request counting for a given rate limit key.
 *
 * <p>This component is responsible for tracking request volume within a fixed
 * time window and resetting state when the window expires.</p>
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class LocalWindowManager {

    private final RateLimiterConfig config;

    public static LocalWindowManager of(RateLimiterConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        return new LocalWindowManager(config);
    }

    public long incrementAndGetRequestsInCurrentWindow(LocalCounterState state, long nowMillis) {
        Objects.requireNonNull(state, "state must not be null");
        moveToNextWindowIfNeeded(state, nowMillis);
        return state.incrementRequestsInCurrentWindow();
    }

    public long computeEffectiveLocalLimit(int limit) {
        return (long) limit + config.getBatchSize();
    }

    /**
     * Ensures that the local time window is up-to-date.
     *
     * <p>If the current time exceeds the configured window duration, this method attempts to
     * advance the window and reset the local request counter.</p>
     *
     * <p>This method is thread-safe and uses a lock-free approach (CAS) to guarantee that only
     * one thread performs the window transition, while others retry until they observe the
     * updated state.</p>
     *
     * @param state     the per-key local counter state
     * @param nowMillis the current timestamp in milliseconds
     */
    private void moveToNextWindowIfNeeded(LocalCounterState state, long nowMillis) {
        long windowDurationMillis = getWindowDurationMillis();

        while (isWindowExpired(state, nowMillis, windowDurationMillis)) {
            long currentWindowStartedAt = state.getCurrentWindowStartedAtMillis();

            if (state.tryMoveToNextWindow(currentWindowStartedAt, nowMillis)) {
                state.resetRequestsInCurrentWindow();
                return;
            }

            // Another thread may advance the window first, so retry until the state is up-to-date.
        }
    }

    private long getWindowDurationMillis() {
        return config.getWindowSeconds() * 1000L;
    }

    private boolean isWindowExpired(LocalCounterState state, long nowMillis, long windowDurationMillis) {
        long windowStart = state.getCurrentWindowStartedAtMillis();
        return nowMillis - windowStart >= windowDurationMillis;
    }

}