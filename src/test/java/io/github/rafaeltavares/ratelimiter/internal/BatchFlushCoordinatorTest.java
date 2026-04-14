package io.github.rafaeltavares.ratelimiter.internal;

import io.github.rafaeltavares.ratelimiter.RateLimiterConfig;
import io.github.rafaeltavares.ratelimiter.store.DistributedKeyValueStore;
import io.github.rafaeltavares.ratelimiter.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class BatchFlushCoordinatorTest {

    private DistributedKeyValueStore keyValueStore;
    private MutableClock clock;
    private RateLimiterConfig config;
    private BatchFlushCoordinator batchFlushCoordinator;

    @BeforeEach
    void setUp() throws Exception {
        keyValueStore = mock(DistributedKeyValueStore.class);
        when(keyValueStore.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(1));

        clock = MutableClock.startingAt(1_000L);

        config = RateLimiterConfig.of(
                3,
                Duration.ofMillis(100),
                4,
                60
        );

        batchFlushCoordinator = BatchFlushCoordinator.of(keyValueStore, config, clock);
    }

    @Test
    void shouldNotFlushWhenBatchSizeAndFlushIntervalWereNotReached() throws Exception {
        LocalCounterState state = new LocalCounterState(clock.millis());
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        batchFlushCoordinator.maybeFlush("client-a", state);

        verify(keyValueStore, never()).incrementByAndExpire(anyString(), anyInt(), anyInt());
    }

    @Test
    void shouldFlushWhenBatchSizeIsReached() throws Exception {
        LocalCounterState state = new LocalCounterState(clock.millis());
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        batchFlushCoordinator.maybeFlush("client-a", state);

        verify(keyValueStore, times(1))
                .incrementByAndExpire(eq("client-a:0"), eq(3), eq(config.getWindowSeconds()));
    }

    @Test
    void shouldFlushWhenFlushIntervalIsReachedEvenBelowBatchSize() throws Exception {
        LocalCounterState state = new LocalCounterState(clock.millis());
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        clock.advanceMillis(150);

        batchFlushCoordinator.maybeFlush("client-a", state);

        verify(keyValueStore, times(1))
                .incrementByAndExpire(eq("client-a:0"), eq(2), eq(config.getWindowSeconds()));
    }

    @Test
    void shouldUseNextShardOnSubsequentFlushes() throws Exception {
        LocalCounterState state = new LocalCounterState(clock.millis());

        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        batchFlushCoordinator.maybeFlush("client-a", state);

        state.incrementPendingBatchCount();
        clock.advanceMillis(150);

        batchFlushCoordinator.maybeFlush("client-a", state);

        verify(keyValueStore, times(1))
                .incrementByAndExpire(eq("client-a:0"), eq(3), eq(config.getWindowSeconds()));

        verify(keyValueStore, times(1))
                .incrementByAndExpire(eq("client-a:1"), eq(1), eq(config.getWindowSeconds()));
    }

    @Test
    void shouldNotStartAnotherFlushWhenFlushIsAlreadyRunning() throws Exception {
        LocalCounterState state = new LocalCounterState(clock.millis());
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        state.tryStartFlush();

        batchFlushCoordinator.maybeFlush("client-a", state);

        verify(keyValueStore, never()).incrementByAndExpire(anyString(), anyInt(), anyInt());
    }
}