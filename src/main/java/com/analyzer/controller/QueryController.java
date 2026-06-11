package com.analyzer.controller;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.analyzer.queue.IngestionPipeline;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api")
public class QueryController {

    private final IngestionPipeline pipeline;
    private final DependencyGraph graph;

    public QueryController(IngestionPipeline pipeline, DependencyGraph graph) {
        this.pipeline = pipeline;
        this.graph = graph;
    }

    /**
     * Publishes a new dependency event to the ingestion queue.
     */
    @PostMapping("/events")
    public ResponseEntity<?> publishEvent(@RequestBody Event event) {
        if (event == null || event.type() == null) {
            return buildErrorResponse("Invalid event payload", HttpStatus.BAD_REQUEST);
        }
        try {
            pipeline.publish(event);
            return ResponseEntity.ok(Map.of("event_id", event.event_id(), "status", "queued"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return buildErrorResponse("Interrupted while queuing event", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException e) {
            return buildErrorResponse("Queue has been shut down", HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Publishes multiple events in a single request.
     */
    @PostMapping("/events/batch")
    public ResponseEntity<?> publishBatch(@RequestBody List<Event> events) {
        if (events == null || events.isEmpty()) {
            return buildErrorResponse("Request body must contain a non-empty list of events", HttpStatus.BAD_REQUEST);
        }
        int queued = 0;
        for (Event event : events) {
            try {
                pipeline.publish(event);
                queued++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return buildErrorResponse("Interrupted after queuing " + queued + " of " + events.size() + " events",
                        HttpStatus.INTERNAL_SERVER_ERROR);
            } catch (IllegalStateException e) {
                return buildErrorResponse("Queue shut down after queuing " + queued + " events",
                        HttpStatus.SERVICE_UNAVAILABLE);
            }
        }
        return ResponseEntity.ok(Map.of("queued", queued, "total", events.size()));
    }

    /**
     * Loads a dataset file through the ingestion pipeline using multiple concurrent
     * producer threads. This endpoint demonstrates the multi-producer queue
     * pattern.
     */
    @PostMapping("/events/load")
    public ResponseEntity<?> loadDataset(@RequestParam String file) {
        if (file == null || file.trim().isEmpty()) {
            return buildErrorResponse("Parameter 'file' is required", HttpStatus.BAD_REQUEST);
        }
        try {
            IngestionPipeline.DatasetLoadResult result = pipeline.loadDataset(file);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return buildErrorResponse("Failed to load dataset: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Gets all downstream reachable services from a given service, with the path.
     */
    @GetMapping("/reachable")
    public ResponseEntity<?> getReachable(@RequestParam String service) {
        if (service == null || service.trim().isEmpty()) {
            return buildErrorResponse("Parameter 'service' is required", HttpStatus.BAD_REQUEST);
        }
        if (!graph.getNodes().containsKey(service)) {
            return buildErrorResponse("Service '" + service + "' not found", HttpStatus.NOT_FOUND);
        }
        Map<String, List<String>> reachable = graph.getReachable(service);
        return ResponseEntity.ok(reachable);
    }

    /**
     * Gets all upstream services that transitively depend on the given service.
     */
    @GetMapping("/dependents")
    public ResponseEntity<?> getDependents(@RequestParam String service) {
        if (service == null || service.trim().isEmpty()) {
            return buildErrorResponse("Parameter 'service' is required", HttpStatus.BAD_REQUEST);
        }
        if (!graph.getNodes().containsKey(service)) {
            return buildErrorResponse("Service '" + service + "' not found", HttpStatus.NOT_FOUND);
        }
        Set<String> dependents = graph.getDependents(service);
        return ResponseEntity.ok(Map.of("service", service, "dependents", dependents));
    }

    /**
     * Calculates the lowest-latency path between source and target.
     */
    @GetMapping("/shortest-path")
    public ResponseEntity<?> getShortestPath(@RequestParam String source, @RequestParam String target) {
        if (source == null || target == null || source.trim().isEmpty() || target.trim().isEmpty()) {
            return buildErrorResponse("Parameters 'source' and 'target' are required", HttpStatus.BAD_REQUEST);
        }
        if (!graph.getNodes().containsKey(source)) {
            return buildErrorResponse("Source service '" + source + "' not found", HttpStatus.NOT_FOUND);
        }
        if (!graph.getNodes().containsKey(target)) {
            return buildErrorResponse("Target service '" + target + "' not found", HttpStatus.NOT_FOUND);
        }

        DependencyGraph.ShortestPathResult result = graph.getShortestPath(source, target);
        if (result == null) {
            return buildErrorResponse("No path exists between " + source + " and " + target, HttpStatus.NOT_FOUND);
        }
        return ResponseEntity.ok(result);
    }

    /**
     * Returns top k critical services in the graph based on Transitive Impact
     * Centrality.
     */
    @GetMapping("/critical-services")
    public ResponseEntity<?> getCriticalServices(@RequestParam(defaultValue = "10") int k) {
        if (k <= 0) {
            return buildErrorResponse("Parameter 'k' must be greater than zero", HttpStatus.BAD_REQUEST);
        }
        List<DependencyGraph.CriticalityScore> scores = graph.getCriticalServices(k);
        return ResponseEntity.ok(scores);
    }

    /**
     * Returns all cycles present in the dependency graph.
     */
    @GetMapping("/cycles")
    public ResponseEntity<?> getCycles() {
        List<List<String>> cycles = graph.getCycles();
        return ResponseEntity.ok(Map.of("cycle_count", cycles.size(), "cycles", cycles));
    }

    /**
     * Returns error rate and p95 latency of edges incident to the service over the
     * trailing window.
     */
    @GetMapping("/health")
    public ResponseEntity<?> getHealth(
            @RequestParam String service,
            @RequestParam(name = "window_seconds", defaultValue = "300") int windowSeconds) {
        if (service == null || service.trim().isEmpty()) {
            return buildErrorResponse("Parameter 'service' is required", HttpStatus.BAD_REQUEST);
        }
        if (windowSeconds <= 0) {
            return buildErrorResponse("Parameter 'window_seconds' must be greater than zero", HttpStatus.BAD_REQUEST);
        }
        if (!graph.getNodes().containsKey(service)) {
            return buildErrorResponse("Service '" + service + "' not found", HttpStatus.NOT_FOUND);
        }

        DependencyGraph.HealthMetrics health = graph.getHealth(service, windowSeconds);
        return ResponseEntity.ok(health);
    }

    /**
     * Exposes server state, queue telemetry, and graph sizes.
     */
    @GetMapping("/metrics")
    public ResponseEntity<?> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("queue_depth", pipeline.getQueueDepth());
        metrics.put("queue_capacity", pipeline.getQueueCapacity());
        metrics.put("producer_count", pipeline.getProducerCount());
        metrics.put("consumer_count", pipeline.getConsumerCount());
        metrics.put("events_published", pipeline.getEventsPublished());
        metrics.put("events_processed", pipeline.getEventsProcessed());
        metrics.put("events_deduplicated", pipeline.getDuplicateEvents());
        metrics.put("graph_service_count", graph.getNodes().size());

        // Count active edges
        int edgeCount = 0;
        for (Map<String, ?> targetMap : graph.getEdges().values()) {
            edgeCount += targetMap.size();
        }
        metrics.put("graph_edge_count", edgeCount);

        return ResponseEntity.ok(metrics);
    }

    private ResponseEntity<Map<String, String>> buildErrorResponse(String message, HttpStatus status) {
        return ResponseEntity.status(status).body(Map.of(
                "error", message,
                "status", String.valueOf(status.value()),
                "timestamp", String.valueOf(System.currentTimeMillis())));
    }
}
