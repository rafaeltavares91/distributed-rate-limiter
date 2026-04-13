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

        long localCount = state.incrementLocalWindowCount();
        state.incrementPendingDelta();

        maybeFlush(key, state);

        long effectiveLocalLimit = computeEffectiveLocalLimit(limit);

        return CompletableFuture.completedFuture(localCount <= effectiveLocalLimit);
    }

    private void refreshWindowIfNeeded(LocalCounterState state, long nowMillis) {
        long windowDurationMillis = config.getWindowSeconds() * 1000L;

        while (true) {
            long currentWindowStart = state.windowStartMillis();

            if (nowMillis - currentWindowStart < windowDurationMillis) {
                return;
            }

            if (state.tryAdvanceWindow(currentWindowStart, nowMillis)) {
                state.resetLocalWindowCount();
                return;
            }
        }
    }

    private long computeEffectiveLocalLimit(int limit) {
        return (long) limit + config.getBatchSize();
    }

    private void maybeFlush(String key, LocalCounterState state) {
        long nowMillis = clock.millis();
        long pendingDelta = state.pendingDelta();

        if (pendingDelta <= 0) {
            return;
        }

        boolean batchThresholdReached = pendingDelta >= config.getBatchSize();
        boolean flushIntervalReached =
                nowMillis - state.lastFlushMillis() >= config.getFlushInterval().toMillis();

        if (!batchThresholdReached && !flushIntervalReached) {
            return;
        }

        if (!state.tryStartFlush()) {
            return;
        }

        long deltaToFlush = state.drainPendingDelta();

        if (deltaToFlush <= 0) {
            state.finishFlush();
            return;
        }

        if (deltaToFlush > Integer.MAX_VALUE) {
            state.finishFlush();
            throw new IllegalStateException("deltaToFlush exceeds integer range");
        }

        int shardSequence = state.nextShardSequence();
        String shardKey = shardStrategy.nextShardKey(key, shardSequence);

        final CompletableFuture<Integer> flushFuture;
        try {
            flushFuture = keyValueStore.incrementByAndExpire(
                    shardKey,
                    (int) deltaToFlush,
                    config.getWindowSeconds()
            );
        } catch (Exception exception) {
            state.finishFlush();
            return;
        }

        flushFuture.whenComplete((ignored, throwable) -> {
            if (throwable == null) {
                state.updateLastFlushMillis(clock.millis());
            }

            state.finishFlush();

            if (state.pendingDelta() > 0) {
                maybeFlush(key, state);
            }
        });
    }

}