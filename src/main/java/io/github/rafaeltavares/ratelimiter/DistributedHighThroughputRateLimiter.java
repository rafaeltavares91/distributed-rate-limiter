package io.github.rafaeltavares.ratelimiter;

import io.github.rafaeltavares.ratelimiter.internal.BatchFlushCoordinator;
import io.github.rafaeltavares.ratelimiter.internal.LocalCounterState;
import io.github.rafaeltavares.ratelimiter.store.DistributedKeyValueStore;
import lombok.Getter;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-throughput distributed rate limiter that uses local estimation for allow/deny decisions
 * and asynchronously flushes aggregated counters to a distributed key-value store.
 */
@Getter
public final class DistributedHighThroughputRateLimiter {

    private final RateLimiterConfig config;
    private final Clock clock;
    private final ConcurrentHashMap<String, LocalCounterState> counters;
    private final BatchFlushCoordinator batchFlushCoordinator;

    private DistributedHighThroughputRateLimiter(
            RateLimiterConfig config,
            Clock clock,
            ConcurrentHashMap<String, LocalCounterState> counters,
            BatchFlushCoordinator batchFlushCoordinator
    ) {
        this.config = config;
        this.clock = clock;
        this.counters = counters;
        this.batchFlushCoordinator = batchFlushCoordinator;
    }

    public static DistributedHighThroughputRateLimiter of(
            DistributedKeyValueStore keyValueStore,
            RateLimiterConfig config,
            Clock clock
    ) {
        Objects.requireNonNull(keyValueStore, "keyValueStore must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        BatchFlushCoordinator batchFlushCoordinator = BatchFlushCoordinator.of(keyValueStore, config, clock);

        return new DistributedHighThroughputRateLimiter(
                config,
                clock,
                new ConcurrentHashMap<>(),
                batchFlushCoordinator
        );
    }

    public static DistributedHighThroughputRateLimiter of(
            DistributedKeyValueStore keyValueStore,
            RateLimiterConfig config
    ) {
        return of(keyValueStore, config, Clock.systemUTC());
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

        long requestsInCurrentWindow = state.incrementRequestsInWindow(
                nowMillis,
                windowDurationMillis()
        );

        state.incrementPendingBatchCount();
        batchFlushCoordinator.maybeFlush(key, state);

        long effectiveLocalLimit = computeEffectiveLocalLimit(limit);

        return CompletableFuture.completedFuture(requestsInCurrentWindow <= effectiveLocalLimit);
    }

    private long computeEffectiveLocalLimit(int limit) {
        return (long) limit + config.getBatchSize();
    }

    private long windowDurationMillis() {
        return config.getWindowSeconds() * 1000L;
    }

}