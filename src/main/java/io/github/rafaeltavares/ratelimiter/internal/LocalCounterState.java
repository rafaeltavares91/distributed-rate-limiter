package io.github.rafaeltavares.ratelimiter.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Holds the local per-key state used by the rate limiter.
 *
 * <p>This class keeps all in-memory information needed to:
 * - track how many requests happened in the current time window
 * - buffer requests before flushing to the distributed store
 * - coordinate flush execution safely across threads
 * - distribute writes across shards</p>
 */
public final class LocalCounterState {

    /**
     * Number of requests accumulated locally that have not yet been flushed
     * to the distributed store. Uses LongAdder for high-throughput increments.
     */
    private final LongAdder pendingBatchCount = new LongAdder();

    /**
     * Timestamp (in milliseconds) of the last successful flush to the distributed store.
     * Used to determine if the flush interval has been exceeded.
     */
    private final AtomicLong lastFlushAtMillis;

    /**
     * Counter used to select the next shard index.
     * Ensures requests are distributed across multiple shard keys.
     */
    private final AtomicInteger nextShardCounter = new AtomicInteger(0);

    /**
     * Indicates whether a flush operation is currently in progress.
     * Prevents multiple concurrent flushes for the same key.
     */
    private final AtomicBoolean flushRunning = new AtomicBoolean(false);

    /**
     * Number of requests observed in the current local time window.
     */
    private final AtomicLong requestsInCurrentWindow = new AtomicLong(0L);

    /**
     * Timestamp (in milliseconds) marking the start of the current time window.
     */
    private final AtomicLong currentWindowStartedAtMillis;

    public LocalCounterState(long initialTimestampMillis) {
        this.lastFlushAtMillis = new AtomicLong(initialTimestampMillis);
        this.currentWindowStartedAtMillis = new AtomicLong(initialTimestampMillis);
    }

    public void incrementPendingBatchCount() {
        pendingBatchCount.increment();
    }

    public long getPendingBatchCount() {
        return pendingBatchCount.sum();
    }

    public long drainPendingBatchCount() {
        return pendingBatchCount.sumThenReset();
    }

    public boolean hasPendingBatch() {
        return getPendingBatchCount() > 0;
    }

    public boolean reachedBatchSize(int batchSize) {
        return getPendingBatchCount() >= batchSize;
    }

    public long getLastFlushAtMillis() {
        return lastFlushAtMillis.get();
    }

    public void markFlushAt(long timestampMillis) {
        lastFlushAtMillis.set(timestampMillis);
    }

    public boolean flushIntervalExceeded(long nowMillis, long flushIntervalMillis) {
        return nowMillis - getLastFlushAtMillis() >= flushIntervalMillis;
    }

    public int nextShardCounter() {
        return nextShardCounter.getAndIncrement();
    }

    public boolean tryStartFlush() {
        return flushRunning.compareAndSet(false, true);
    }

    public void finishFlush() {
        flushRunning.set(false);
    }

    /**
     * Advances the local time window when it has expired and returns the updated
     * request count for the current window.
     *
     * <p>This method is thread-safe and uses CAS to ensure that only one thread
     * performs the window transition.</p>
     *
     * @param nowMillis current timestamp in milliseconds
     * @param windowDurationMillis configured window duration in milliseconds
     * @return the incremented request count for the active local window
     */
    public long incrementRequestsInWindow(long nowMillis, long windowDurationMillis) {
        moveToNextWindowIfNeeded(nowMillis, windowDurationMillis);
        return requestsInCurrentWindow.incrementAndGet();
    }

    private void moveToNextWindowIfNeeded(long nowMillis, long windowDurationMillis) {
        while (isWindowExpired(nowMillis, windowDurationMillis)) {
            long currentWindowStartedAt = currentWindowStartedAtMillis.get();

            if (currentWindowStartedAtMillis.compareAndSet(currentWindowStartedAt, nowMillis)) {
                requestsInCurrentWindow.set(0L);
                return;
            }

            // Another thread may advance the window first, so retry until the state is up-to-date.
        }
    }

    private boolean isWindowExpired(long nowMillis, long windowDurationMillis) {
        long windowStart = currentWindowStartedAtMillis.get();
        return nowMillis - windowStart >= windowDurationMillis;
    }
}