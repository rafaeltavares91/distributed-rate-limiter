package io.github.rafaeltavares.ratelimiter;

import lombok.Getter;

import java.time.Duration;
import java.util.Objects;

@Getter
public final class RateLimiterConfig {

    private final int batchSize;
    private final Duration flushInterval;
    private final int shardCount; // number of subkeys
    private final int windowSeconds; // rate limit duration window

    private RateLimiterConfig(
            int batchSize,
            Duration flushInterval,
            int shardCount,
            int windowSeconds
    ) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        Objects.requireNonNull(flushInterval, "flushInterval must not be null");

        if (flushInterval.isZero() || flushInterval.isNegative()) {
            throw new IllegalArgumentException("flushInterval must be greater than zero");
        }
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be greater than zero");
        }
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("windowSeconds must be greater than zero");
        }
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.shardCount = shardCount;
        this.windowSeconds = windowSeconds;
    }

    public static RateLimiterConfig of(
            int batchSize,
            Duration flushInterval,
            int shardCount,
            int windowSeconds
    ) {
        return new RateLimiterConfig(batchSize, flushInterval, shardCount, windowSeconds);
    }

    public static RateLimiterConfig defaultConfig() {
        return RateLimiterConfig.of(
                100,
                Duration.ofMillis(100),
                16,
                60
        );
    }

}