package io.github.rafaeltavares.ratelimiter.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

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

    public long getCurrentWindowStartedAtMillis() {
        return currentWindowStartedAtMillis.get();
    }

    public boolean tryMoveToNextWindow(long expectedStartMillis, long newStartMillis) {
        return currentWindowStartedAtMillis.compareAndSet(expectedStartMillis, newStartMillis);
    }

    public void resetRequestsInCurrentWindow() {
        requestsInCurrentWindow.set(0L);
    }

    public long incrementRequestsInCurrentWindow() {
        return requestsInCurrentWindow.incrementAndGet();
    }

}