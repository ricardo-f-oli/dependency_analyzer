# Service Dependency Analyzer

A high-performance, concurrent service dependency analyzer in Java 21 and Spring Boot. It maintains an in-memory directed graph of services, processes streams through a custom blocking queue, supports snapshotting and WAL durability, and exposes REST APIs for graph queries.

---

## 🛠️ Build and Run Instructions

### Prerequisites
* **Java SDK**: JDK 17 or 21 configured.
* **Maven**: Maven installed and available in PATH.

### 1. Compile and Run Tests
Run the compiler and test suites from the project root directory:
```bash
mvn clean test
```

### 2. Start the Service
Start the REST API server (runs on port `8080` by default):
```bash
mvn spring-boot:run
```

### 3. Generate Large Dataset
To generate a synthetic testing stream containing 100,000 events and 6,000 services, run:
```bash
mvn compile exec:java "-Dexec.mainClass=com.analyzer.generator.DataGenerator"
```

---

## 🔌 API Documentation & Invocations

### 1. Submit Event
Publish dependency/telemetry events manually:
```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "e-9f2c-41bc",
    "type": "dependency_observed",
    "timestamp": "2026-06-10T10:00:00.000Z",
    "source": "checkout-api",
    "target": "payments-service",
    "latency_ms": 42,
    "status": "ok"
  }'
```

### 2. Reachable (Blast Radius)
Get all downstream services reachable from a given service, with path details:
```bash
curl "http://localhost:8080/api/reachable?service=checkout-api"
```

### 3. Dependents (Transitive Downstream Impact)
Find all services that transitively depend on the given service:
```bash
curl "http://localhost:8080/api/dependents?service=payments-service"
```

### 4. Shortest Path (Weighted by Average Latency)
Find the path of lowest average latency using Dijkstra's algorithm:
```bash
curl "http://localhost:8080/api/shortest-path?source=checkout-api&target=payments-service"
```

### 5. Critical Services
Find the top `k` services whose deletion would disconnect the most service pairs (based on Transitive Impact Centrality):
```bash
curl "http://localhost:8080/api/critical-services?k=5"
```

### 6. Cycles Detection
Find all cycles currently existing in the graph:
```bash
curl "http://localhost:8080/api/cycles"
```

### 7. Health Check
Retrieve rolling average error rate and p95 latency on incident edges over a trailing window:
```bash
curl "http://localhost:8080/api/health?service=checkout-api&window_seconds=300"
```

### 8. Metrics
Fetch system telemetry including queue depth, processing stats, and graph size:
```bash
curl "http://localhost:8080/api/metrics"
```

---

## ⚙️ Configuration Knobs

Configurable properties inside `src/main/resources/application.yml`:
* `server.port`: HTTP server port (default `8080`).
* `app.queue.capacity`: Maximum capacity of the bounded queue (default `100000`).
* `app.queue.consumers`: Number of parallel consumer threads processing queue events (default `4`).
* `app.persistence.snapshot-interval-events`: Event interval cadence before writing snapshot files (default `20000`).
* `app.persistence.snapshot-file`: Storage path for snapshot files.
* `app.persistence.wal-file`: Storage path for the write-ahead event log.

---

## 📌 Assumptions and Constraints

This implementation makes several key assumptions and enforces specific constraints:

### Key Assumptions
1. **Event Data Integrity**: Events must have unique `event_id` values to ensure deduplication works correctly.
2. **Memory Constraints**: The entire dependency graph must fit in memory (single-node deployment).
3. **Single-Node Deployment**: This is designed for a single JVM instance and doesn't include distributed coordination features.
4. **Event Ordering**: Events are processed in chronological order, with late-arriving events being dropped if they contain timestamps older than the last processed update time.

### Key Constraints
1. **Graph Size Limitation**: Due to in-memory storage, graphs larger than available RAM cannot be handled without modifications.
2. **Concurrency Model**: Uses a bounded thread pool for consumer threads with a fixed number of workers (default 4).
3. **Persistence Model**: Implements a dual persistence model using Write-Ahead Log (WAL) and periodic snapshots.
4. **Query Performance**: Optimized for read-heavy workloads with Reader-Writer Locks for concurrent access.

---

## 🐳 Containerized Deployment

### Prerequisites
* **Docker** installed and available in PATH
* **Docker Compose** installed and available in PATH

### 1. Build and Run with Docker Compose
To build and run the service using Docker Compose:
```bash
docker-compose up --build
```

### 2. Stop the Service
To stop the running service:
```bash
docker-compose down
```

### 3. Test the Application
Once the service is running, you can test it using curl commands:

#### Health Check
```bash
curl http://localhost:8080/actuator/health
```

#### Submit Event
```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "e-9f2c-41bc",
    "type": "dependency_observed",
    "timestamp": "2026-06-10T10:00:00.000Z",
    "source": "checkout-api",
    "target": "payments-service",
    "latency_ms": 42,
    "status": "ok"
  }'
```

#### Get Reachable Services
```bash
curl "http://localhost:8080/api/reachable?service=checkout-api"
```

#### Get Dependents
```bash
curl "http://localhost:8080/api/dependents?service=payments-service"
```

#### Get Shortest Path
```bash
curl "http://localhost:8080/api/shortest-path?source=checkout-api&target=payments-service"
```

#### Get Critical Services
```bash
curl "http://localhost:8080/api/critical-services?k=5"
```

#### Find Cycles
```bash
curl "http://localhost:8080/api/cycles"
```

#### Get Health Metrics
```bash
curl "http://localhost:8080/api/health?service=checkout-api&window_seconds=300"
```

#### Get System Metrics
```bash
curl "http://localhost:8080/api/metrics"
```

