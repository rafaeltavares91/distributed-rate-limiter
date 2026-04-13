package io.github.rafaeltavares.ratelimiter.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class MutableClock extends Clock {

    private final AtomicLong currentMillis;
    private final ZoneId zoneId;

    public MutableClock(long initialMillis, ZoneId zoneId) {
        this.currentMillis = new AtomicLong(initialMillis);
        this.zoneId = Objects.requireNonNull(zoneId);
    }

    public static MutableClock startingAt(long initialMillis) {
        return new MutableClock(initialMillis, ZoneId.of("UTC"));
    }

    public void advanceMillis(long millis) {
        currentMillis.addAndGet(millis);
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(currentMillis.get(), zone);
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(currentMillis.get());
    }

    @Override
    public long millis() {
        return currentMillis.get();
    }
}