package io.github.rafaeltavares.ratelimiter.internal;

public record ShardStrategy(int shardCount) {

    public ShardStrategy {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be greater than zero");
        }
    }

    public String shardKey(String key, int shardIndex) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
        return key + ":" + shardIndex;
    }

    public String nextShardKey(String key, int sequence) {
        int index = selectShardIndex(sequence);
        return shardKey(key, index);
    }

    public int selectShardIndex(int sequence) {
        return Math.floorMod(sequence, shardCount);
    }

}