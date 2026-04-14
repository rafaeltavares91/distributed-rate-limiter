package io.github.rafaeltavares.ratelimiter;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Duration;
import java.util.Objects;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class RateLimiterConfig {

    private final int batchSize;
    private final Duration flushInterval;
    private final int shardCount;
    private final int windowSeconds;

    public static RateLimiterConfig of(
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

        return new RateLimiterConfig(
                batchSize,
                flushInterval,
                shardCount,
                windowSeconds
        );
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