# Service Dependency Analyzer

A high-performance, concurrent service dependency analyzer in Java 21 and Spring Boot. It maintains an in-memory directed graph of services, processes streams through a custom blocking queue, supports snapshotting and WAL durability, and exposes REST APIs for graph queries.

---

## 🛠️ Build and Run Instructions

### Prerequisites
* **Java SDK**: JDK 17 or 21 configured.
* **Maven**: (A pre-downloaded Maven binary is available at `C:\Users\ricar\.gemini\antigravity-ide\scratch\maven\apache-maven-3.9.6`).

### 1. Compile and Run Tests
Run the compiler and test suites from the project root directory:
```powershell
$env:Path = "C:\Users\ricar\Downloads\openjdk-21+35_windows-x64_bin\jdk-21\bin;C:\Users\ricar\.gemini\antigravity-ide\scratch\maven\apache-maven-3.9.6\bin;" + $env:Path
mvn clean test
```

### 2. Start the Service
Start the REST API server (runs on port `8080` by default):
```powershell
$env:Path = "C:\Users\ricar\Downloads\openjdk-21+35_windows-x64_bin\jdk-21\bin;C:\Users\ricar\.gemini\antigravity-ide\scratch\maven\apache-maven-3.9.6\bin;" + $env:Path
mvn spring-boot:run
```

### 3. Generate Large Dataset
To generate a synthetic testing stream containing 100,000 events and 6,000 services, run:
```powershell
$env:Path = "C:\Users\ricar\Downloads\openjdk-21+35_windows-x64_bin\jdk-21\bin;C:\Users\ricar\.gemini\antigravity-ide\scratch\maven\apache-maven-3.9.6\bin;" + $env:Path
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
