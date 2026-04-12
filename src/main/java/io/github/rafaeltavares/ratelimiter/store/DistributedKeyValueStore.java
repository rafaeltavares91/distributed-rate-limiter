package io.github.rafaeltavares.ratelimiter;

import java.util.concurrent.CompletableFuture;

public interface DistributedKeyValueStore {
    CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds) throws Exception;
}
