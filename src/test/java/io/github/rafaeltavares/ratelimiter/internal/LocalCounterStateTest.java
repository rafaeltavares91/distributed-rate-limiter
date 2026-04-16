package io.github.rafaeltavares.ratelimiter.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LocalCounterStateTest {

    @Test
    void shouldIncrementRequestsWithinSameWindow() {
        LocalCounterState state = new LocalCounterState(1_000L);
        long windowDurationMillis = 60_000L;

        long first = state.incrementRequestsInWindow(1_000L, windowDurationMillis);
        long second = state.incrementRequestsInWindow(1_100L, windowDurationMillis);
        long third = state.incrementRequestsInWindow(1_200L, windowDurationMillis);

        assertEquals(1L, first);
        assertEquals(2L, second);
        assertEquals(3L, third);
    }

    @Test
    void shouldResetWindowWhenWindowExpires() {
        LocalCounterState state = new LocalCounterState(1_000L);
        long windowDurationMillis = 60_000L;

        assertEquals(1L, state.incrementRequestsInWindow(1_000L, windowDurationMillis));
        assertEquals(2L, state.incrementRequestsInWindow(1_100L, windowDurationMillis));

        long afterReset = state.incrementRequestsInWindow(61_001L, windowDurationMillis);

        assertEquals(1L, afterReset);
    }

    @Test
    void shouldIncrementPendingBatchCount() {
        LocalCounterState state = new LocalCounterState(1_000L);

        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        assertEquals(2L, state.getPendingBatchCount());
    }

    @Test
    void shouldDrainPendingBatchCount() {
        LocalCounterState state = new LocalCounterState(1_000L);

        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        long drained = state.drainPendingBatchCount();

        assertEquals(3L, drained);
        assertEquals(0L, state.getPendingBatchCount());
    }

    @Test
    void shouldReportWhenThereIsPendingBatch() {
        LocalCounterState state = new LocalCounterState(1_000L);

        assertFalse(state.hasPendingBatch());

        state.incrementPendingBatchCount();

        assertTrue(state.hasPendingBatch());
    }

    @Test
    void shouldReportWhenBatchSizeWasReached() {
        LocalCounterState state = new LocalCounterState(1_000L);

        state.incrementPendingBatchCount();
        state.incrementPendingBatchCount();

        assertFalse(state.reachedBatchSize(3));

        state.incrementPendingBatchCount();

        assertTrue(state.reachedBatchSize(3));
    }

    @Test
    void shouldReportWhenFlushIntervalWasExceeded() {
        LocalCounterState state = new LocalCounterState(1_000L);

        assertFalse(state.flushIntervalExceeded(1_050L, 100L));
        assertTrue(state.flushIntervalExceeded(1_100L, 100L));
        assertTrue(state.flushIntervalExceeded(1_150L, 100L));
    }

    @Test
    void shouldAdvanceShardCounter() {
        LocalCounterState state = new LocalCounterState(1_000L);

        assertEquals(0, state.nextShardCounter());
        assertEquals(1, state.nextShardCounter());
        assertEquals(2, state.nextShardCounter());
    }
}