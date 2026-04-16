package io.github.rafaeltavares.ratelimiter.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Holds the local per-key state used by the rate limiter.
 *
 * <p>This includes:
 * - the number of requests seen in the current local time window
 * - the number of locally buffered requests pending flush
 * - flush coordination state
 * - shard sequencing for distributed writes</p>
 */
public final class LocalCounterState {

    private final LongAdder pendingBatchCount = new LongAdder();
    private final AtomicLong lastFlushAtMillis;
    private final AtomicInteger nextShardCounter = new AtomicInteger(0);
    private final AtomicBoolean flushRunning = new AtomicBoolean(false);

    private final AtomicLong requestsInCurrentWindow = new AtomicLong(0L);
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

    public long getLastFlushAtMillis() {
        return lastFlushAtMillis.get();
    }

    public void markFlushAt(long timestampMillis) {
        lastFlushAtMillis.set(timestampMillis);
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