# Backend Engineer Take-Home: Service Dependency Analyzer

## Overview

Build a small backend system that ingests a stream of service-dependency events, maintains an in-memory directed graph of the services in a fictional production environment, and answers analytical queries against that graph.

The motivating use case is **incident response**: when a service degrades, on-call engineers need to know — quickly — what depends on it, what it depends on, and how the failure could propagate. You're building the engine that powers those questions.

### Domain Primer (the only context you need)

In a distributed system, services call other services. Each call is a directed edge: `A → B` means "service A makes requests to service B." When B is unhealthy, every service upstream of B is at risk. When A is misbehaving, everything *downstream* of A may be feeling it.

Services emit dependency events as they observe traffic. A typical event looks like:

```json
{
  "event_id": "e-9f2c...",
  "type": "dependency_observed",
  "timestamp": "2026-05-06T14:21:09.412Z",
  "source": "checkout-api",
  "target": "payments-service",
  "latency_ms": 42,
  "status": "ok"
}
```

You will see at least the following event types:

| Type | Meaning |
|---|---|
| `dependency_observed` | `source` called `target`. Carries `latency_ms` and `status` (`ok` \| `error` \| `timeout`). |
| `dependency_removed` | A previously observed edge should be removed (e.g. a service was decommissioned). |
| `service_metadata` | Attaches/updates attributes on a service node (`team`, `tier`, `region`, etc.). |
| `heartbeat` | Liveness signal from a service. Absence over a window means the service is considered stale. |

Events may arrive **out of order**, **duplicated**, or **interleaved from multiple producers**. Your system must tolerate that.

---

## The Task

Build a backend service that does the following:

1. **Simulates a message queue.** Multiple producers publish events; multiple consumers drain them. You may *not* use Kafka, RabbitMQ, NATS, Redis Streams, SQS, or any other off-the-shelf queue. Build it from primitives (channels, blocking queues, mutex-protected ring buffers — your call).
2. **Maintains an in-memory directed graph** of services and their dependencies, updated as events are processed. You may *not* use Neo4j, Memgraph, NetworkX, or any other graph library/database. Build the graph from standard data structures.
3. **Exposes a query API** (HTTP or gRPC — your choice) that answers the analytical questions below.
4. **Persists state** durably enough that the process can restart without losing the graph.

### Required Queries

Your query API must support at minimum:

- **`reachable(service)`** — every service reachable downstream of the given service, with the path. This is the "blast radius" question.
- **`dependents(service)`** — every service that transitively depends on the given service (i.e. reverse reachability). This is the "who is affected if this goes down" question.
- **`shortest_path(source, target)`** — the lowest-latency path between two services, using a recent rolling average of `latency_ms` per edge as the weight. Return the path and the total weight, or indicate no path exists.
- **`critical_services(k)`** — the top *k* services that, if removed, would disconnect the most pairs of services. Define and document your criticality metric; betweenness-style centrality is one reasonable choice but not the only one.
- **`cycles()`** — all dependency cycles currently present in the graph.
- **`health(service, window)`** — over the trailing `window` (e.g. last 5 minutes), the error rate and p95 latency of edges incident to the service.

How you shape the request/response payloads is up to you, but the API should be one a teammate could integrate against without reading your source.

---

## Functional Requirements

### Must-haves

**Ingestion pipeline**
- At least 2 concurrent producers and 2 concurrent consumers, configurable.
- Idempotent processing — duplicate `event_id`s must not corrupt the graph.
- Out-of-order tolerant — late `dependency_removed` events for edges that haven't arrived yet should not crash the system or leave it in an inconsistent state.
- Backpressure — when consumers fall behind, producers must block or shed load deliberately, not silently drop events. Document your choice.
- Graceful shutdown — on SIGTERM, drain in-flight events, snapshot state, and exit cleanly.

**Graph state**
- Thread-safe. Reads (queries) and writes (event application) run concurrently and must not tear or deadlock.
- Persisted via snapshot, event-log replay, or both. After a restart, queries must return the same answers as before the restart, given the same input stream.

**Query API**
- Reachable over HTTP or gRPC.
- Returns structured errors (unknown service, malformed request, etc.) — not stack traces.
- Reasonable latency on the dataset you generate (single-digit ms for point queries on a graph of ~10k services / ~100k edges is a good north star, but tell us what you actually achieved).

**Tests**
- Enough automated coverage to convince a reviewer the core invariants hold: idempotency, concurrent correctness, query correctness on hand-crafted graphs (including ones with cycles), restart consistency.

### Nice-to-haves (pick any you have time for)

- **Streaming subscriptions** — clients can subscribe to graph changes (e.g. "tell me when `payments-service`'s blast radius changes").
- **Time-travel queries** — "what did the graph look like 10 minutes ago?"
- **Anomaly detection** — flag edges whose latency or error rate has shifted significantly versus their recent baseline.
- **Horizontal-scale design note** — a short section in the report on how you'd shard or replicate this if the event rate were 100×.
- **Containerized deployment** — a `docker compose up` that brings the whole thing up.

---

## Data

You generate your own dataset. We're looking for something that exercises the system meaningfully:

- ~5,000–10,000 services, ~50,000–200,000 events.
- Realistic shape: a few high-fan-in services (databases, auth), most services with modest fan-out, at least a couple of cycles, a long tail of latencies, a non-trivial error rate.
- Mix of all event types — including removals and metadata updates — interleaved.

Commit the dataset (or the generator) to the repo. A thoughtful dataset will make your queries more interesting to evaluate.

---

## Technical Requirements

- **Language / runtime:** any. Pick what you'd reach for in production and be ready to explain why. We have engineers comfortable in Go, Rust, Python, Java, Kotlin, TypeScript, and C++.
- **External dependencies:** standard libraries and ergonomic helpers (HTTP/gRPC framework, logging, serialization, test runners) are fine. **No external message queues, no graph databases, no graph algorithm libraries** — implement the queue, the graph, and the algorithms yourself. Persistence may use SQLite, an embedded KV store, or just files on disk; your call.
- **Concurrency:** must be genuinely concurrent, not a sequential loop with `async` sprinkled on it.
- **Observability:** structured logs are required; metrics (counters for events processed, queue depth, query latency histograms) are strongly encouraged. We should be able to tell what the system is doing while it runs.

---

## Deliverables

1. **Source repository** (GitHub or similar) including your dataset or generator.
2. **Documentation** in the repo covering:
   - How to build and run the system (one command preferred).
   - How to publish events and how to query the API, with example invocations.
   - Configuration knobs (producer/consumer counts, queue capacity, snapshot cadence, etc.).
   - Assumptions you made and constraints you chose to enforce.
3. **Short report (1–2 pages)** covering:
   - Architecture and the main design decisions: queue model, graph representation, concurrency strategy, persistence approach.
   - Your criticality metric and why you chose it.
   - Trade-offs you made — especially anywhere you chose simplicity over scale, or vice versa.
   - What you'd build next with more time, and how you'd evolve this toward a real production service (sharding, durability guarantees, multi-tenant isolation, etc.).
   - Anything you're particularly proud of, or that didn't go the way you hoped.

---

## Evaluation Criteria

We weigh these roughly equally:

- **Correctness** — do the queries return the right answers, including on graphs with cycles, disconnected components, and concurrent updates?
- **Concurrency & systems thinking** — is the ingestion pipeline genuinely concurrent and safe? Are backpressure, idempotency, and shutdown handled deliberately?
- **Code quality** — structure, readability, idiomatic use of your stack, sensible abstractions, no dead weight.
- **Algorithmic judgment** — appropriate choices of graph representations and algorithms for the queries you implement, with reasonable complexity.
- **Operability** — can a teammate run this, observe it, and trust it? Logs, metrics, errors, configuration.
- **Communication** — clarity and honesty of the report. Tell us what you cut and why.

A smaller, well-built, polished system beats a feature-stuffed but rough one. We're looking for the engineer we'd want owning a service in production, not the one who can cram the most checkboxes into a weekend.

---

## Time and Scope

We expect this to take **6–8 hours of focused work.** Please don't go significantly past that. If you catch yourself wanting to, stop and note in your report what you would have done with more time — we'd rather see good scope judgment than maximum output.

---

Good luck, and have fun with it.
