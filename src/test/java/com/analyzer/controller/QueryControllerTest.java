package com.analyzer.controller;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class QueryControllerTest {

        @Autowired
        private MockMvc mockMvc;

        @Autowired
        private DependencyGraph graph;

        @BeforeEach
        void cleanGraph() {
                graph.clear();
                // Also clear the processed event IDs to avoid interference from snapshot
                // This ensures test isolation
                graph.getProcessedEventIds().clear();
        }

        @Test
        void testEventIngestionAndTelemetryApi() throws Exception {
                // Direct inject event so it's instantly queryable
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "auth-service", "db-service",
                                50, "ok", null);
                graph.applyEvent(e1);

                // Fetch Metrics
                mockMvc.perform(get("/api/metrics"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.graph_service_count").value(2))
                                .andExpect(jsonPath("$.graph_edge_count").value(1));

                // Fetch Reachable
                mockMvc.perform(get("/api/reachable").param("service", "auth-service"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.db-service").isArray());

                // Fetch Reachable non-existent (should return 404)
                mockMvc.perform(get("/api/reachable").param("service", "non-existent"))
                                .andExpect(status().isNotFound())
                                .andExpect(jsonPath("$.error").value("Service 'non-existent' not found"));
        }

        @Test
        void testQueryValidationErrors() throws Exception {
                // Missing parameter
                mockMvc.perform(get("/api/reachable").param("service", ""))
                                .andExpect(status().isBadRequest())
                                .andExpect(jsonPath("$.error").value("Parameter 'service' is required"));

                // Invalid shortest path request (missing params)
                mockMvc.perform(get("/api/shortest-path").param("source", "a"))
                                .andExpect(status().isBadRequest());
        }

        @Test
        void testShortestPathResult() throws Exception {
                // Setup nodes
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "s1", "s2", 10, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "s2", "s3", 5, "ok", null);
                graph.applyEvent(e1);
                graph.applyEvent(e2);

                mockMvc.perform(get("/api/shortest-path")
                                .param("source", "s1")
                                .param("target", "s3"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.path[0]").value("s1"))
                                .andExpect(jsonPath("$.path[1]").value("s2"))
                                .andExpect(jsonPath("$.path[2]").value("s3"))
                                .andExpect(jsonPath("$.totalWeight").value(15.0));
        }

        @Test
        void testGetDependents() throws Exception {
                // Setup nodes
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "auth-service", "db-service",
                                50, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "web-app", "auth-service", 30,
                                "ok", null);
                graph.applyEvent(e1);
                graph.applyEvent(e2);

                mockMvc.perform(get("/api/dependents").param("service", "auth-service"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.service").value("auth-service"))
                                .andExpect(jsonPath("$.dependents").isArray());
        }

        @Test
        void testGetCriticalServices() throws Exception {
                // Setup nodes with dependencies to make critical services meaningful
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "auth-service", "db-service",
                                50, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "web-app", "auth-service", 30,
                                "ok", null);
                Event e3 = new Event("t3", "dependency_observed", "2026-06-10T00:00:00Z", "api-gateway", "auth-service",
                                20, "ok", null);
                graph.applyEvent(e1);
                graph.applyEvent(e2);
                graph.applyEvent(e3);

                mockMvc.perform(get("/api/critical-services").param("k", "5"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$").isArray());
        }

        @Test
        void testGetCycles() throws Exception {
                // Setup a cycle
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "a", "b", 10, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "b", "c", 10, "ok", null);
                Event e3 = new Event("t3", "dependency_observed", "2026-06-10T00:00:00Z", "c", "a", 10, "ok", null);
                graph.applyEvent(e1);
                graph.applyEvent(e2);
                graph.applyEvent(e3);

                mockMvc.perform(get("/api/cycles"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.cycle_count").value(1))
                                .andExpect(jsonPath("$.cycles").isArray());
        }

        @Test
        void testGetCriticalServicesWithMultipleDependencies() throws Exception {
                // Setup nodes with multiple dependencies to make critical services meaningful
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "auth-service", "db-service",
                                50, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "web-app", "auth-service", 30,
                                "ok", null);
                Event e3 = new Event("t3", "dependency_observed", "2026-06-10T00:00:00Z", "api-gateway", "auth-service",
                                20, "ok", null);
                Event e4 = new Event("t4", "dependency_observed", "2026-06-10T00:00:00Z", "frontend", "web-app", 25,
                                "ok", null);
                Event e5 = new Event("t5", "dependency_observed", "2026-06-10T00:00:00Z", "backend", "web-app", 40,
                                "ok", null);
                graph.applyEvent(e1);
                graph.applyEvent(e2);
                graph.applyEvent(e3);
                graph.applyEvent(e4);
                graph.applyEvent(e5);

                mockMvc.perform(get("/api/critical-services").param("k", "5"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$").isArray())
                                .andExpect(jsonPath("$.length()").value(5));
        }

        @Test
        void testGetCyclesWithDisconnectedComponents() throws Exception {
                // Setup disconnected components: two isolated subgraphs
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "a", "b", 10, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "b", "c", 10, "ok", null);
                // Cycle in first component
                Event e3 = new Event("t3", "dependency_observed", "2026-06-10T00:00:00Z", "c", "a", 10, "ok", null);

                // Second isolated subgraph with no connections to first
                Event e4 = new Event("t4", "dependency_observed", "2026-06-10T00:00:00Z", "x", "y", 15, "ok", null);
                Event e5 = new Event("t5", "dependency_observed", "2026-06-10T00:00:00Z", "y", "z", 15, "ok", null);
                // Cycle in second component
                Event e6 = new Event("t6", "dependency_observed", "2026-06-10T00:00:00Z", "z", "x", 15, "ok", null);

                graph.applyEvent(e1);
                graph.applyEvent(e2);
                graph.applyEvent(e3);
                graph.applyEvent(e4);
                graph.applyEvent(e5);
                graph.applyEvent(e6);

                mockMvc.perform(get("/api/cycles"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.cycle_count").value(2))
                                .andExpect(jsonPath("$.cycles").isArray())
                                .andExpect(jsonPath("$.cycles.length()").value(2));
        }

        @Test
        void testGetDependentsWithMultipleDependencies() throws Exception {
                // Setup nodes with complex dependencies - make sure we have exactly 3
                // dependents
                Event e1 = new Event("t1", "dependency_observed", "2026-06-10T00:00:00Z", "auth-service", "db-service",
                                50, "ok", null);
                Event e2 = new Event("t2", "dependency_observed", "2026-06-10T00:00:00Z", "web-app", "auth-service", 30,
                                "ok", null);
                Event e3 = new Event("t3", "dependency_observed", "2026-06-10T00:00:00Z", "api-gateway", "auth-service",
                                20, "ok", null);
                // Don't add frontend as a dependent of auth-service to keep count at 2
                graph.applyEvent(e1);
                graph.applyEvent(e2);
                graph.applyEvent(e3);

                mockMvc.perform(get("/api/dependents").param("service", "auth-service"))
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.service").value("auth-service"))
                                .andExpect(jsonPath("$.dependents").isArray())
                                .andExpect(jsonPath("$.dependents.length()").value(2));
        }
}
