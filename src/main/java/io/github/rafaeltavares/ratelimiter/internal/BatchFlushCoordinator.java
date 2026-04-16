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
 *
 * <p>This component is responsible for deciding when a local batch should be flushed,
 * sending the aggregated count to the distributed key-value store, and ensuring that
 * only one flush per key runs at a time.</p>
 *
 * <p>It also distributes writes across shard keys in order to reduce hot key and
 * hot partition issues under heavy load.</p>
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

    /**
     * Attempts to flush the local buffered request count for a given key.
     *
     * <p>A flush is triggered only when one of the configured conditions is met:</p>
     * <ul>
     *   <li>the buffered batch size reaches the configured threshold</li>
     *   <li>the configured flush interval has been exceeded</li>
     * </ul>
     *
     * <p>If no flush is needed, this method returns immediately.</p>
     *
     * <p>If another flush is already running for the same key, this method also returns
     * immediately to avoid duplicate concurrent flushes.</p>
     *
     * <p>When a flush happens, the buffered request count is drained, sent asynchronously
     * to the distributed store, and recorded under a shard key to reduce contention.</p>
     *
     * @param key the logical rate limit key being flushed
     * @param state the local in-memory state associated with the key
     */
    public void maybeFlush(String key, LocalCounterState state) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(state, "state must not be null");

        if (!shouldFlush(state)) {
            return;
        }

        if (!state.tryStartFlush()) {
            return;
        }

        flushPendingBatch(key, state);
    }

    /**
     * Returns true when the buffered state should be flushed, either because the local
     * batch size threshold was reached or because the flush interval has elapsed.
     */
    private boolean shouldFlush(LocalCounterState state) {
        long nowMillis = clock.millis();

        if (!state.hasPendingBatch()) {
            return false;
        }

        return state.reachedBatchSize(config.getBatchSize())
                || state.flushIntervalExceeded(nowMillis, config.getFlushInterval().toMillis());
    }

    /**
     * Drains the current local batch and sends it to the distributed store.
     */
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

        String shardKey = shardStrategy.nextShardKey(key, state.nextShardCounter());

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

    /**
     * Finalizes the current flush and, if new buffered requests arrived while the flush
     * was running, attempts another flush immediately.
     */
    private void handleFlushCompletion(String key, LocalCounterState state, Throwable throwable) {
        if (throwable == null) {
            state.markFlushAt(clock.millis());
        }

        state.finishFlush();

        if (state.hasPendingBatch()) {
            maybeFlush(key, state);
        }
    }
}