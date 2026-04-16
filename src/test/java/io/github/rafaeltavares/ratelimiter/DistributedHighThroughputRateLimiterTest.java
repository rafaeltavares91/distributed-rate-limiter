package io.github.rafaeltavares.ratelimiter;

import io.github.rafaeltavares.ratelimiter.store.DistributedKeyValueStore;
import io.github.rafaeltavares.ratelimiter.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DistributedHighThroughputRateLimiterTest {

    private MutableClock clock;
    private RateLimiterConfig config;
    private DistributedHighThroughputRateLimiter rateLimiter;

    @BeforeEach
    void setUp() throws Exception {
        DistributedKeyValueStore keyValueStore = mock(DistributedKeyValueStore.class);
        when(keyValueStore.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(1));

        clock = MutableClock.startingAt(1_000L);

        config = RateLimiterConfig.of(
                3,
                Duration.ofMillis(100),
                4,
                60
        );

        rateLimiter = DistributedHighThroughputRateLimiter.of(keyValueStore, config, clock);
    }

    @Test
    void shouldThrowWhenKeyIsNull() {
        assertThrows(NullPointerException.class, () -> rateLimiter.isAllowed(null, 10));
    }

    @Test
    void shouldThrowWhenKeyIsBlank() {
        assertThrows(IllegalArgumentException.class, () -> rateLimiter.isAllowed("   ", 10));
    }

    @Test
    void shouldReturnFalseWhenLimitIsZeroOrNegative() {
        assertFalse(rateLimiter.isAllowed("client-a", 0).join());
        assertFalse(rateLimiter.isAllowed("client-a", -1).join());
    }

    @Test
    void shouldAllowRequestsUpToEffectiveLocalLimit() {
        int limit = 5;
        long effectiveLocalLimit = limit + config.getBatchSize();

        for (int i = 0; i < effectiveLocalLimit; i++) {
            assertTrue(rateLimiter.isAllowed("client-a", limit).join());
        }
    }

    @Test
    void shouldDenyRequestsAboveEffectiveLocalLimit() {
        int limit = 5;
        long effectiveLocalLimit = limit + config.getBatchSize();

        for (int i = 0; i < effectiveLocalLimit; i++) {
            assertTrue(rateLimiter.isAllowed("client-a", limit).join());
        }

        assertFalse(rateLimiter.isAllowed("client-a", limit).join());
    }

    @Test
    void shouldResetWindowAfterConfiguredDuration() {
        int limit = 2;
        long effectiveLocalLimit = limit + config.getBatchSize();

        for (int i = 0; i < effectiveLocalLimit; i++) {
            assertTrue(rateLimiter.isAllowed("client-a", limit).join());
        }

        assertFalse(rateLimiter.isAllowed("client-a", limit).join());

        clock.advanceMillis(config.getWindowSeconds() * 1000L + 1);

        assertTrue(rateLimiter.isAllowed("client-a", limit).join());
    }
}