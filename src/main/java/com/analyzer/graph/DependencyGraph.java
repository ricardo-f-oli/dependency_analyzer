package com.analyzer.graph;

import com.analyzer.model.EdgeMetrics;
import com.analyzer.model.Event;
import com.analyzer.model.Observation;
import com.analyzer.model.ServiceMetadata;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.stereotype.Component;

@Component
public class DependencyGraph {

    // Node Metadata
    private final Map<String, ServiceMetadata> nodes = new ConcurrentHashMap<>();

    // Adjacency Lists for graph traversal (protected by ReadWriteLock)
    private final Map<String, Set<String>> adj = new HashMap<>();
    private final Map<String, Set<String>> revAdj = new HashMap<>();

    // Edge metrics: source -> target -> EdgeMetrics
    private final Map<String, Map<String, EdgeMetrics>> edges = new ConcurrentHashMap<>();

    // Out-of-order execution prevention: edgeKey -> lastProcessedEventTimestamp
    private final Map<String, Instant> edgeLastUpdated = new ConcurrentHashMap<>();

    // ReadWriteLock for controlling concurrency
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    // Idempotency: Keep track of processed event IDs
    private final Set<String> processedEventIds = Collections.newSetFromMap(
            new LinkedHashMap<String, Boolean>(10000, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > 200000; // Keep up to 200,000 events to prevent memory overflow
                }
            });

    /**
     * Applies an incoming event to the graph structure.
     * Thread-safe: Acquires the write lock.
     */
    public boolean applyEvent(Event event) {
        if (event == null || event.event_id() == null) {
            return false;
        }

        Instant timestamp;
        try {
            timestamp = Instant.parse(event.timestamp());
        } catch (Exception e) {
            timestamp = Instant.now();
        }

        // Idempotency check - moved inside write lock for consistency
        writeLock.lock();
        try {
            if (processedEventIds.contains(event.event_id())) {
                return false; // Skip duplicate event
            }
            processedEventIds.add(event.event_id());
        } finally {
            writeLock.unlock();
        }

        switch (event.type()) {
            case Event.TYPE_OBSERVED:
                return handleDependencyObserved(event, timestamp);
            case Event.TYPE_REMOVED:
                return handleDependencyRemoved(event, timestamp);
            case Event.TYPE_METADATA:
                return handleServiceMetadata(event);
            case Event.TYPE_HEARTBEAT:
                return handleHeartbeat(event, timestamp);
            default:
                return false;
        }
    }

    private boolean handleDependencyObserved(Event event, Instant timestamp) {
        String source = event.source();
        String target = event.target();
        if (source == null || target == null)
            return false;

        String edgeKey = source + "->" + target;
        // Atomic out-of-order check using compute() to prevent TOCTOU race.
        // Two threads doing get-then-put could both pass the stale check and both
        // write.
        boolean[] stale = { false };
        edgeLastUpdated.compute(edgeKey, (key, existing) -> {
            if (existing != null && !timestamp.isAfter(existing)) {
                stale[0] = true;
                return existing; // keep the newer timestamp
            }
            return timestamp;
        });
        if (stale[0]) {
            return false;
        }

        // Ensure nodes exist
        nodes.computeIfAbsent(source, ServiceMetadata::new);
        nodes.computeIfAbsent(target, ServiceMetadata::new);

        writeLock.lock();
        try {
            adj.computeIfAbsent(source, k -> new HashSet<>()).add(target);
            revAdj.computeIfAbsent(target, k -> new HashSet<>()).add(source);

            EdgeMetrics metrics = edges.computeIfAbsent(source, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(target, k -> new EdgeMetrics(source, target));

            metrics.addObservation(new Observation(timestamp, event.latency_ms() != null ? event.latency_ms() : 0,
                    event.status() != null ? event.status() : "ok"));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private boolean handleDependencyRemoved(Event event, Instant timestamp) {
        String source = event.source();
        String target = event.target();
        if (source == null || target == null)
            return false;

        String edgeKey = source + "->" + target;
        // Atomic out-of-order check using compute() to prevent TOCTOU race
        boolean[] stale = { false };
        edgeLastUpdated.compute(edgeKey, (key, existing) -> {
            if (existing != null && !timestamp.isAfter(existing)) {
                stale[0] = true;
                return existing;
            }
            return timestamp;
        });
        if (stale[0]) {
            return false;
        }

        writeLock.lock();
        try {
            if (adj.containsKey(source)) {
                adj.get(source).remove(target);
                if (adj.get(source).isEmpty()) {
                    adj.remove(source);
                }
            }
            if (revAdj.containsKey(target)) {
                revAdj.get(target).remove(source);
                if (revAdj.get(target).isEmpty()) {
                    revAdj.remove(target);
                }
            }
            if (edges.containsKey(source)) {
                edges.get(source).remove(target);
                if (edges.get(source).isEmpty()) {
                    edges.remove(source);
                }
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private boolean handleServiceMetadata(Event event) {
        String source = event.source();
        if (source == null)
            return false;

        writeLock.lock();
        try {
            ServiceMetadata metadata = nodes.computeIfAbsent(source, ServiceMetadata::new);
            metadata.updateAttributes(event.metadata());
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private boolean handleHeartbeat(Event event, Instant timestamp) {
        String source = event.source();
        if (source == null)
            return false;

        // Acquire write lock for consistency with handleServiceMetadata and to ensure
        // visibility of the lastHeartbeat write to threads holding the read lock
        writeLock.lock();
        try {
            ServiceMetadata metadata = nodes.computeIfAbsent(source, ServiceMetadata::new);
            metadata.setLastHeartbeat(timestamp);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns downstream reachable services with the path taken.
     */
    public Map<String, List<String>> getReachable(String service) {
        readLock.lock();
        try {
            Map<String, List<String>> result = new HashMap<>();
            if (!nodes.containsKey(service)) {
                return result;
            }

            Queue<String> queue = new LinkedList<>();
            Map<String, String> parent = new HashMap<>();
            Set<String> visited = new HashSet<>();

            queue.add(service);
            visited.add(service);

            while (!queue.isEmpty()) {
                String current = queue.poll();
                Set<String> neighbors = adj.get(current);
                if (neighbors != null) {
                    for (String neighbor : neighbors) {
                        if (!visited.contains(neighbor)) {
                            visited.add(neighbor);
                            parent.put(neighbor, current);
                            queue.add(neighbor);
                        }
                    }
                }
            }

            // Build path for each visited service (excluding starting service)
            for (String target : visited) {
                if (target.equals(service))
                    continue;
                List<String> path = new ArrayList<>();
                String curr = target;
                while (curr != null) {
                    path.add(curr);
                    curr = parent.get(curr);
                }
                Collections.reverse(path);
                result.put(target, path);
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns all services that transitively depend on the given service.
     */
    public Set<String> getDependents(String service) {
        readLock.lock();
        try {
            Set<String> result = new HashSet<>();
            if (!nodes.containsKey(service)) {
                return result;
            }

            Queue<String> queue = new LinkedList<>();
            queue.add(service);

            while (!queue.isEmpty()) {
                String current = queue.poll();
                Set<String> upstreams = revAdj.get(current);
                if (upstreams != null) {
                    for (String upstream : upstreams) {
                        if (!result.contains(upstream) && !upstream.equals(service)) {
                            result.add(upstream);
                            queue.add(upstream);
                        }
                    }
                }
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Calculates the shortest path between two nodes using Dijkstra's algorithm.
     * Weights are determined by rolling average edge latencies.
     */
    public ShortestPathResult getShortestPath(String source, String target) {
        readLock.lock();
        try {
            if (!nodes.containsKey(source) || !nodes.containsKey(target)) {
                return null;
            }

            Map<String, Double> distances = new HashMap<>();
            Map<String, String> parent = new HashMap<>();
            PriorityQueue<PathNode> pq = new PriorityQueue<>(Comparator.comparingDouble(PathNode::distance));

            distances.put(source, 0.0);
            pq.add(new PathNode(source, 0.0));

            while (!pq.isEmpty()) {
                PathNode currNode = pq.poll();
                String u = currNode.name();
                double d = currNode.distance();

                if (d > distances.getOrDefault(u, Double.MAX_VALUE))
                    continue;
                if (u.equals(target))
                    break;

                Set<String> neighbors = adj.get(u);
                if (neighbors != null) {
                    for (String v : neighbors) {
                        double weight = getEdgeWeight(u, v);
                        double newDist = d + weight;
                        if (newDist < distances.getOrDefault(v, Double.MAX_VALUE)) {
                            distances.put(v, newDist);
                            parent.put(v, u);
                            pq.add(new PathNode(v, newDist));
                        }
                    }
                }
            }

            if (!distances.containsKey(target)) {
                return null; // unreachable
            }

            List<String> path = new ArrayList<>();
            String curr = target;
            while (curr != null) {
                path.add(curr);
                curr = parent.get(curr);
            }
            Collections.reverse(path);
            return new ShortestPathResult(path, distances.get(target));
        } finally {
            readLock.unlock();
        }
    }

    private double getEdgeWeight(String source, String target) {
        Map<String, EdgeMetrics> targetMap = edges.get(source);
        if (targetMap != null) {
            EdgeMetrics metrics = targetMap.get(target);
            if (metrics != null) {
                return metrics.getRollingAverageLatency();
            }
        }
        return 1.0;
    }

    /**
     * Detects all cycle paths in the graph using DFS with lexicographical order
     * constraints
     * to prevent duplicates, capped at 100 paths total to avoid CPU/memory blowup.
     */
    public List<List<String>> getCycles() {
        readLock.lock();
        try {
            List<List<String>> cycles = new ArrayList<>();
            List<String> sortedNodes = new ArrayList<>(nodes.keySet());
            Collections.sort(sortedNodes);

            // Capped at 100 cycles to maintain low latencies on complex graphs
            int maxCycles = 100;

            for (int i = 0; i < sortedNodes.size(); i++) {
                if (cycles.size() >= maxCycles)
                    break;
                String startNode = sortedNodes.get(i);
                List<String> path = new ArrayList<>();
                Set<String> pathSet = new HashSet<>();
                findCyclesDFS(startNode, startNode, path, pathSet, cycles, maxCycles, sortedNodes, i);
            }

            return cycles;
        } finally {
            readLock.unlock();
        }
    }

    private void findCyclesDFS(String startNode, String current, List<String> path, Set<String> pathSet,
            List<List<String>> cycles, int maxCycles, List<String> sortedNodes, int startIndex) {
        if (cycles.size() >= maxCycles)
            return;

        path.add(current);
        pathSet.add(current);

        Set<String> neighbors = adj.get(current);
        if (neighbors != null) {
            for (String neighbor : neighbors) {
                if (neighbor.equals(startNode)) {
                    // Cycle found!
                    List<String> cycle = new ArrayList<>(path);
                    cycle.add(startNode);
                    cycles.add(cycle);
                    if (cycles.size() >= maxCycles)
                        break;
                } else if (!pathSet.contains(neighbor)) {
                    // Only visit neighbors that are lexicographically greater than startNode to
                    // prevent duplicates
                    if (neighbor.compareTo(startNode) > 0) {
                        findCyclesDFS(startNode, neighbor, path, pathSet, cycles, maxCycles, sortedNodes, startIndex);
                    }
                }
            }
        }

        pathSet.remove(current);
        path.remove(path.size() - 1);
    }

    /**
     * Returns top k critical services using Transitive Impact Centrality (TIC).
     * TIC(s) = |Dependents(s)| * |Reachable(s)|
     */
    public List<CriticalityScore> getCriticalServices(int k) {
        readLock.lock();
        try {
            List<CriticalityScore> scores = nodes.keySet().parallelStream()
                    .map(node -> {
                        long reachCount = countReachable(node);
                        long depCount = countDependents(node);
                        long score = reachCount * depCount;
                        return new CriticalityScore(node, score);
                    })
                    .collect(java.util.stream.Collectors.toList());

            scores.sort(Comparator.comparingLong(CriticalityScore::score).reversed());
            return scores.subList(0, Math.min(k, scores.size()));
        } finally {
            readLock.unlock();
        }
    }

    private long countReachable(String service) {
        // Simple BFS count
        Queue<String> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();
        queue.add(service);
        visited.add(service);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            Set<String> neighbors = adj.get(current);
            if (neighbors != null) {
                for (String neighbor : neighbors) {
                    if (!visited.contains(neighbor)) {
                        visited.add(neighbor);
                        queue.add(neighbor);
                    }
                }
            }
        }
        return visited.size() - 1; // exclude self
    }

    private long countDependents(String service) {
        // Simple BFS count on reverse graph
        Queue<String> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();
        queue.add(service);
        visited.add(service);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            Set<String> upstreams = revAdj.get(current);
            if (upstreams != null) {
                for (String upstream : upstreams) {
                    if (!visited.contains(upstream)) {
                        visited.add(upstream);
                        queue.add(upstream);
                    }
                }
            }
        }
        return visited.size() - 1; // exclude self
    }

    public Map<String, ServiceMetadata> getNodes() {
        return nodes;
    }

    public Map<String, Map<String, EdgeMetrics>> getEdges() {
        return edges;
    }

    public Set<String> getProcessedEventIds() {
        synchronized (processedEventIds) {
            return new HashSet<>(processedEventIds);
        }
    }

    public Map<String, Instant> getEdgeLastUpdated() {
        return new HashMap<>(edgeLastUpdated);
    }

    public void addProcessedEventId(String eventId) {
        synchronized (processedEventIds) {
            processedEventIds.add(eventId);
        }
    }

    public void putEdgeLastUpdated(String edgeKey, Instant timestamp) {
        edgeLastUpdated.put(edgeKey, timestamp);
    }

    public void restoreEdge(String source, String target, List<Observation> observations) {
        writeLock.lock();
        try {
            adj.computeIfAbsent(source, k -> new HashSet<>()).add(target);
            revAdj.computeIfAbsent(target, k -> new HashSet<>()).add(source);
            EdgeMetrics metrics = edges.computeIfAbsent(source, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(target, k -> new EdgeMetrics(source, target));
            for (Observation obs : observations) {
                metrics.addObservation(obs);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void clear() {
        writeLock.lock();
        try {
            nodes.clear();
            adj.clear();
            revAdj.clear();
            edges.clear();
            edgeLastUpdated.clear();
            processedEventIds.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public HealthMetrics getHealth(String service, int windowSeconds) {
        readLock.lock();
        try {
            if (!nodes.containsKey(service)) {
                return new HealthMetrics(service, 0.0, 0.0, 0);
            }

            Instant cutoff = Instant.now().minusSeconds(windowSeconds);
            List<Observation> allObs = new ArrayList<>();

            // Find incoming and outgoing edges for the service
            // Outgoing edges
            Map<String, EdgeMetrics> outgoing = edges.get(service);
            if (outgoing != null) {
                for (EdgeMetrics edge : outgoing.values()) {
                    List<Observation> obsFromEdge = edge.getObservationsSince(cutoff);
                    allObs.addAll(obsFromEdge);
                }
            }

            // Incoming edges
            for (Map<String, EdgeMetrics> targetMap : edges.values()) {
                EdgeMetrics incomingEdge = targetMap.get(service);
                if (incomingEdge != null) {
                    List<Observation> obsFromEdge = incomingEdge.getObservationsSince(cutoff);
                    allObs.addAll(obsFromEdge);
                }
            }

            if (allObs.isEmpty()) {
                return new HealthMetrics(service, 0.0, 0.0, 0);
            }

            int totalCount = allObs.size();
            int errorCount = 0;
            List<Integer> latencies = new ArrayList<>();

            for (Observation obs : allObs) {
                latencies.add(obs.latencyMs());
                if ("error".equals(obs.status()) || "timeout".equals(obs.status())) {
                    errorCount++;
                }
            }

            double errorRate = (double) errorCount / totalCount;
            Collections.sort(latencies);

            // Handle edge case where we have only one observation
            int p95Index = Math.min((int) Math.ceil(0.95 * totalCount) - 1, latencies.size() - 1);
            double p95Latency = latencies.get(p95Index);

            return new HealthMetrics(service, errorRate, p95Latency, totalCount);
        } finally {
            readLock.unlock();
        }
    }

    public record ShortestPathResult(List<String> path, double totalWeight) {
    }

    public record CriticalityScore(String service, long score) {
    }

    public record HealthMetrics(String service, double errorRate, double p95LatencyMs, int sampleCount) {
    }

    private record PathNode(String name, double distance) {
    }
}
