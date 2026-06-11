package com.analyzer.graph;

import com.analyzer.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class DependencyGraphTest {

    private DependencyGraph graph;

    @BeforeEach
    void setUp() {
        graph = new DependencyGraph();
    }

    @Test
    void testBasicReachableAndDependents() {
        // Create graph: A -> B -> C
        Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:00Z", "A", "B", 10, "ok", null);
        Event e2 = new Event("e2", "dependency_observed", "2026-06-10T00:00:01Z", "B", "C", 20, "ok", null);

        graph.applyEvent(e1);
        graph.applyEvent(e2);

        // Reachable
        Map<String, List<String>> reachable = graph.getReachable("A");
        assertThat(reachable).containsKeys("B", "C");
        assertThat(reachable.get("B")).containsExactly("A", "B");
        assertThat(reachable.get("C")).containsExactly("A", "B", "C");

        // Dependents
        Set<String> dependents = graph.getDependents("C");
        assertThat(dependents).containsExactlyInAnyOrder("A", "B");
    }

    @Test
    void testDijkstraShortestPath() {
        // Path 1: A -> B (latency 20)
        Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:00Z", "A", "B", 20, "ok", null);
        // Path 2: A -> C (latency 2) -> B (latency 3)
        Event e2 = new Event("e2", "dependency_observed", "2026-06-10T00:00:01Z", "A", "C", 2, "ok", null);
        Event e3 = new Event("e3", "dependency_observed", "2026-06-10T00:00:02Z", "C", "B", 3, "ok", null);

        graph.applyEvent(e1);
        graph.applyEvent(e2);
        graph.applyEvent(e3);

        DependencyGraph.ShortestPathResult result = graph.getShortestPath("A", "B");
        assertThat(result).isNotNull();
        assertThat(result.path()).containsExactly("A", "C", "B");
        assertThat(result.totalWeight()).isEqualTo(5.0);
    }

    @Test
    void testCycleDetection() {
        // Create cycle: A -> B -> C -> A
        Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:00Z", "A", "B", 5, "ok", null);
        Event e2 = new Event("e2", "dependency_observed", "2026-06-10T00:00:01Z", "B", "C", 5, "ok", null);
        Event e3 = new Event("e3", "dependency_observed", "2026-06-10T00:00:02Z", "C", "A", 5, "ok", null);

        graph.applyEvent(e1);
        graph.applyEvent(e2);
        graph.applyEvent(e3);

        List<List<String>> cycles = graph.getCycles();
        assertThat(cycles).isNotEmpty();
        // Since we extract cycle paths, it should contain a cycle equivalent to [A, B, C, A]
        boolean foundCycle = false;
        for (List<String> cycle : cycles) {
            if (cycle.size() == 4 && cycle.get(0).equals("A") && cycle.get(3).equals("A")) {
                assertThat(cycle).containsExactly("A", "B", "C", "A");
                foundCycle = true;
            }
        }
        assertThat(foundCycle).isTrue();
    }

    @Test
    void testIdempotency() {
        Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:00Z", "A", "B", 15, "ok", null);
        
        boolean applied1 = graph.applyEvent(e1);
        boolean applied2 = graph.applyEvent(e1); // duplicate event_id

        assertThat(applied1).isTrue();
        assertThat(applied2).isFalse();

        DependencyGraph.ShortestPathResult result = graph.getShortestPath("A", "B");
        assertThat(result.totalWeight()).isEqualTo(15.0);
    }

    @Test
    void testOutOfOrderEvents() {
        // e2 (removed) happens at T2
        Event e2 = new Event("e2", "dependency_removed", "2026-06-10T00:00:10Z", "A", "B", null, null, null);
        // e1 (observed) is a late arrival happening at T1
        Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:05Z", "A", "B", 10, "ok", null);

        // Apply T2 (removed) first
        graph.applyEvent(e2);
        // Apply T1 (late observed) second
        graph.applyEvent(e1);

        // Since the removed event has a newer timestamp than the observed event, the edge should remain removed.
        DependencyGraph.ShortestPathResult result = graph.getShortestPath("A", "B");
        assertThat(result).isNull();
    }

    @Test
    void testHealthMonitoringAndSlidingWindow() {
        // e1 is in the window (T=now)
        String nowStr = Instant.now().toString();
        Event e1 = new Event("e1", "dependency_observed", nowStr, "A", "B", 100, "error", null);

        // e2 is outside the window (T = 10 minutes ago)
        String oldStr = Instant.now().minusSeconds(600).toString();
        Event e2 = new Event("e2", "dependency_observed", oldStr, "A", "B", 50, "ok", null);

        graph.applyEvent(e1);
        graph.applyEvent(e2);

        // Health check for window of 5 minutes (300 seconds)
        // Should only count e1
        DependencyGraph.HealthMetrics health = graph.getHealth("A", 300);
        assertThat(health.sampleCount()).isEqualTo(1);
        assertThat(health.errorRate()).isEqualTo(1.0); // 1 error / 1 sample
        assertThat(health.p95LatencyMs()).isEqualTo(100.0);
    }
}
