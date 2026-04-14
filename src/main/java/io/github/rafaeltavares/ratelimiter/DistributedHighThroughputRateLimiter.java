package io.github.rafaeltavares.ratelimiter;

import io.github.rafaeltavares.ratelimiter.internal.LocalCounterState;
import io.github.rafaeltavares.ratelimiter.internal.ShardStrategy;
import io.github.rafaeltavares.ratelimiter.store.DistributedKeyValueStore;
import lombok.Getter;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DistributedHighThroughputRateLimiter {

    @Getter
    private final RateLimiterConfig config;
    @Getter
    private final Clock clock;

    private final DistributedKeyValueStore keyValueStore;
    private final ShardStrategy shardStrategy;
    private final ConcurrentHashMap<String, LocalCounterState> counters = new ConcurrentHashMap<>();

    private DistributedHighThroughputRateLimiter(
            DistributedKeyValueStore keyValueStore,
            RateLimiterConfig config,
            Clock clock
    ) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore, "keyValueStore must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.shardStrategy = new ShardStrategy(config.getShardCount());
    }

    public static DistributedHighThroughputRateLimiter of(
            DistributedKeyValueStore keyValueStore,
            RateLimiterConfig config,
            Clock clock
    ) {
        return new DistributedHighThroughputRateLimiter(keyValueStore, config, clock);
    }

    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        Objects.requireNonNull(key, "key must not be null");

        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }

        if (limit <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        long nowMillis = clock.millis();

        LocalCounterState state = counters.computeIfAbsent(
                key,
                ignored -> new LocalCounterState(nowMillis)
        );

        refreshWindowIfNeeded(state, nowMillis);

        long localCount = state.incrementRequestsInCurrentWindow();
        state.incrementPendingBatchCount();

        maybeFlush(key, state);

        long effectiveLocalLimit = computeEffectiveLocalLimit(limit);

        return CompletableFuture.completedFuture(localCount <= effectiveLocalLimit);
    }

    private void refreshWindowIfNeeded(LocalCounterState state, long nowMillis) {
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

    private long computeEffectiveLocalLimit(int limit) {
        return (long) limit + config.getBatchSize();
    }

    private void maybeFlush(String key, LocalCounterState state) {
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
        boolean flushIntervalReached =
                nowMillis - state.getLastFlushAtMillis() >= config.getFlushInterval().toMillis();

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
            keyValueStore.incrementByAndExpire(
                    shardKey,
                    (int) batchToFlush,
                    config.getWindowSeconds()
            ).whenComplete((ignored, throwable) -> handleFlushCompletion(key, state, throwable));
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