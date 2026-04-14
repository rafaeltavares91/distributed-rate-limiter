package io.github.rafaeltavares.ratelimiter;

import io.github.rafaeltavares.ratelimiter.internal.BatchFlushCoordinator;
import io.github.rafaeltavares.ratelimiter.internal.LocalCounterState;
import io.github.rafaeltavares.ratelimiter.internal.LocalWindowManager;
import io.github.rafaeltavares.ratelimiter.store.DistributedKeyValueStore;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-throughput distributed rate limiter that uses local estimation for allow/deny decisions
 * and asynchronously flushes aggregated counters to a distributed key-value store.
 */

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class DistributedHighThroughputRateLimiter {

    @Getter
    private final RateLimiterConfig config;
    @Getter
    private final Clock clock;

    private final ConcurrentHashMap<String, LocalCounterState> counters;
    private final LocalWindowManager localWindowManager;
    private final BatchFlushCoordinator batchFlushCoordinator;

    public static DistributedHighThroughputRateLimiter of(
            DistributedKeyValueStore keyValueStore,
            RateLimiterConfig config,
            Clock clock
    ) {
        Objects.requireNonNull(keyValueStore, "keyValueStore must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        LocalWindowManager localWindowManager = LocalWindowManager.of(config);
        BatchFlushCoordinator batchFlushCoordinator = BatchFlushCoordinator.of(keyValueStore, config, clock);

        return new DistributedHighThroughputRateLimiter(
                config,
                clock,
                new ConcurrentHashMap<>(),
                localWindowManager,
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

        LocalCounterState state = counters.computeIfAbsent(key, ignored -> new LocalCounterState(nowMillis));

        long requestsInCurrentWindow = localWindowManager.incrementAndGetRequestsInCurrentWindow(state, nowMillis);

        state.incrementPendingBatchCount();
        batchFlushCoordinator.maybeFlush(key, state);

        long effectiveLocalLimit = localWindowManager.computeEffectiveLocalLimit(limit);

        return CompletableFuture.completedFuture(requestsInCurrentWindow <= effectiveLocalLimit);
    }

}