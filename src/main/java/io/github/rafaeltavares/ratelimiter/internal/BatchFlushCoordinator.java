package io.github.rafaeltavares.ratelimiter.internal;

import io.github.rafaeltavares.ratelimiter.RateLimiterConfig;
import io.github.rafaeltavares.ratelimiter.store.DistributedKeyValueStore;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinates asynchronous flushes of locally buffered request counts to the distributed store.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class BatchFlushCoordinator {

    private final DistributedKeyValueStore keyValueStore;
    private final RateLimiterConfig config;
    private final Clock clock;
    private final ShardStrategy shardStrategy;

    public static BatchFlushCoordinator of(
            DistributedKeyValueStore keyValueStore,
            RateLimiterConfig config,
            Clock clock
    ) {
        Objects.requireNonNull(keyValueStore, "keyValueStore must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        return new BatchFlushCoordinator(
                keyValueStore,
                config,
                clock,
                new ShardStrategy(config.getShardCount())
        );
    }

    public void maybeFlush(String key, LocalCounterState state) {
        if (!shouldFlush(state)) {
            return;
        }

        if (!state.tryStartFlush()) {
            return;
        }

        flushPendingBatch(key, state);
    }

    private boolean shouldFlush(LocalCounterState state) {
        long nowMillis = clock.millis();
        long pendingBatchCount = state.getPendingBatchCount();

        if (pendingBatchCount <= 0) {
            return false;
        }

        boolean batchSizeReached = pendingBatchCount >= config.getBatchSize();
        boolean flushIntervalReached = nowMillis - state.getLastFlushAtMillis() >= config.getFlushInterval().toMillis();

        return batchSizeReached || flushIntervalReached;
    }

    private void flushPendingBatch(String key, LocalCounterState state) {
        long batchToFlush = state.drainPendingBatchCount();

        if (batchToFlush <= 0) {
            state.finishFlush();
            return;
        }

        if (batchToFlush > Integer.MAX_VALUE) {
            state.finishFlush();
            throw new IllegalStateException("batchToFlush exceeds integer range");
        }

        String shardKey = nextShardKey(key, state);

        try {
            CompletableFuture<Integer> flushFuture = keyValueStore.incrementByAndExpire(
                    shardKey,
                    (int) batchToFlush,
                    config.getWindowSeconds()
            );

            flushFuture.whenComplete((ignored, throwable) ->
                    handleFlushCompletion(key, state, throwable)
            );
        } catch (Exception exception) {
            state.finishFlush();
        }
    }

    private String nextShardKey(String key, LocalCounterState state) {
        int shardCounter = state.nextShardCounter();
        return shardStrategy.nextShardKey(key, shardCounter);
    }

    private void handleFlushCompletion(String key, LocalCounterState state, Throwable throwable) {
        if (throwable == null) {
            state.markFlushAt(clock.millis());
        }

        state.finishFlush();

        if (state.getPendingBatchCount() > 0) {
            maybeFlush(key, state);
        }
    }

}