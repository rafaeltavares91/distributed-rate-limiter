package io.github.rafaeltavares.ratelimiter.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class LocalCounterState {

    private final LongAdder pendingDelta = new LongAdder();
    private final AtomicLong lastFlushMillis;
    private final AtomicInteger nextShardIndex = new AtomicInteger(0);
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    private LocalCounterState(long initialFlushMillis) {
        this.lastFlushMillis = new AtomicLong(initialFlushMillis);
    }

    public static LocalCounterState of(long initialFlushMillis) {
        return new LocalCounterState(initialFlushMillis);
    }

    public void incrementPendingDelta() {
        pendingDelta.increment();
    }

    public long pendingDelta() {
        return pendingDelta.sum();
    }

    public long drainPendingDelta() {
        return pendingDelta.sumThenReset();
    }

    public long lastFlushMillis() {
        return lastFlushMillis.get();
    }

    public void updateLastFlushMillis(long timestampMillis) {
        lastFlushMillis.set(timestampMillis);
    }

    public int nextShardSequence() {
        return nextShardIndex.getAndIncrement();
    }

    public boolean tryStartFlush() {
        return flushInProgress.compareAndSet(false, true);
    }

    public void finishFlush() {
        flushInProgress.set(false);
    }

    public boolean isFlushInProgress() {
        return flushInProgress.get();
    }

}