package io.github.rafaeltavares.ratelimiter.internal;

import io.github.rafaeltavares.ratelimiter.RateLimiterConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalWindowManagerTest {

    private LocalWindowManager localWindowManager;

    @BeforeEach
    void setUp() {
        RateLimiterConfig config = RateLimiterConfig.of(
                3,
                Duration.ofMillis(100),
                4,
                60
        );

        localWindowManager = LocalWindowManager.of(config);
    }

    @Test
    void shouldIncrementRequestsWithinSameWindow() {
        LocalCounterState state = new LocalCounterState(1_000L);

        long first = localWindowManager.incrementAndGetRequestsInCurrentWindow(state, 1_000L);
        long second = localWindowManager.incrementAndGetRequestsInCurrentWindow(state, 1_100L);
        long third = localWindowManager.incrementAndGetRequestsInCurrentWindow(state, 1_200L);

        assertEquals(1L, first);
        assertEquals(2L, second);
        assertEquals(3L, third);
    }

    @Test
    void shouldResetWindowWhenWindowExpires() {
        LocalCounterState state = new LocalCounterState(1_000L);

        assertEquals(1L, localWindowManager.incrementAndGetRequestsInCurrentWindow(state, 1_000L));
        assertEquals(2L, localWindowManager.incrementAndGetRequestsInCurrentWindow(state, 1_100L));

        long afterReset = localWindowManager.incrementAndGetRequestsInCurrentWindow(state, 61_001L);

        assertEquals(1L, afterReset);
    }

    @Test
    void shouldComputeEffectiveLocalLimitUsingBatchSizeMargin() {
        long effectiveLocalLimit = localWindowManager.computeEffectiveLocalLimit(10);

        assertEquals(13L, effectiveLocalLimit);
    }
}