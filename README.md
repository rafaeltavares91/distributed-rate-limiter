# 🚀 Distributed High Throughput Rate Limiter

## Overview

This project implements a high-throughput distributed rate limiter designed to operate across a fleet of servers.

The solution focuses on:

* High scalability
* Minimal contention on shared resources
* Avoiding hot keys and hot partitions
* Maintaining good performance under heavy load

The design intentionally favors **eventual consistency and high throughput** over strict accuracy.

---

## 🎯 Rate Limiting Strategy

Since we cannot read global state, we use **local approximation**.

### Effective Local Limit

Instead of enforcing:

```
localCount <= limit
```

We allow a margin:

```
localCount <= limit + batchSize
```

### Why?

Because:

* Some requests are buffered locally and not yet flushed
* Other instances may also be accepting requests
* We must avoid blocking too early

This ensures:

* Clients can make **at least** the allowed number of requests
* Small, controlled overshoot is acceptable

---

## ⚙️ Concurrency Model

The implementation is thread-safe using:

* ConcurrentHashMap for key state
* Atomic primitives (AtomicLong, AtomicBoolean, etc.)
* LongAdder for high-throughput increments
* Compare-and-set (CAS) for window rotation

Flush operations are coordinated to avoid duplicate concurrent flushes per key.

---

## ⚖️ Trade-offs

### Accuracy vs Throughput

We deliberately trade strict accuracy for:

* Reduced contention
* Lower latency
* Higher throughput

### No Global Read

Since the store does not support reads:

* We cannot enforce exact limits globally
* We rely on local estimation

### Eventual Consistency

* Local state is flushed asynchronously
* Temporary divergence between nodes is expected

---

## 🧪 Testing

The project includes unit tests covering:

* Input validation
* Allow/Deny behavior
* Window rotation
* Batch flushing
* Time-based flush
* Shard distribution

A custom `MutableClock` is used to simulate time in tests.

---

## ▶️ How to Run Tests

```
mvn clean test
```

---

## 🏁 Conclusion

This implementation provides a practical and scalable solution for distributed rate limiting under heavy load, prioritizing:

* Performance
* Simplicity
* Robustness

while respecting the constraints of the problem.
