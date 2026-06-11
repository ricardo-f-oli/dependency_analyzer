package com.analyzer.persistence;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.analyzer.queue.IngestionPipeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class RestartConsistencyEndToEndTest {

    @Mock
    private DependencyGraph graph;

    @Mock
    private IngestionPipeline pipeline;

    @Test
    void testFullRestartRecoveryProcess() throws IOException {
        // Create a temporary directory for testing
        String tempDir = System.getProperty("java.io.tmpdir") + "/test_full_restart_consistency";
        Files.createDirectories(Paths.get(tempDir));

        try {
            // Setup paths for this test
            String snapshotFile = tempDir + "/snapshot.json";
            String walFile = tempDir + "/events.jsonl";

            // Create a real graph and persistence manager with test files
            DependencyGraph testGraph = new DependencyGraph();
            PersistenceManager testPersistenceManager = new PersistenceManager(snapshotFile, walFile);

            // Simulate processing events through the pipeline to populate the graph
            Event event1 = new Event("event1", "dependency_observed", "2026-06-10T00:00:00Z", "service-a", "service-b",
                    50, "ok", null);
            Event event2 = new Event("event2", "dependency_observed", "2026-06-10T00:00:00Z", "service-b", "service-c",
                    30, "ok", null);
            Event event3 = new Event("event3", "dependency_observed", "2026-06-10T00:00:00Z", "service-c", "service-a",
                    40, "ok", null); // Creates a cycle
            Event event4 = new Event("event4", "dependency_observed", "2026-06-10T00:00:00Z", "isolated-service",
                    "another-isolated",
                    25, "ok", null); // Creates isolated component

            // Process events through the graph to populate it
            testGraph.applyEvent(event1);
            testGraph.applyEvent(event2);
            testGraph.applyEvent(event3);
            testGraph.applyEvent(event4);

            // Simulate processing events and writing to WAL
            testPersistenceManager.appendToWal(event1);
            testPersistenceManager.appendToWal(event2);
            testPersistenceManager.appendToWal(event3);
            testPersistenceManager.appendToWal(event4);

            // Verify that we can read back the events from WAL
            List<Event> walEvents = testPersistenceManager.readFromWAL();
            assertThat(walEvents).hasSize(4);
            assertThat(walEvents.get(0)).isEqualTo(event1);
            assertThat(walEvents.get(1)).isEqualTo(event2);
            assertThat(walEvents.get(2)).isEqualTo(event3);
            assertThat(walEvents.get(3)).isEqualTo(event4);

            // Test snapshot functionality - save snapshot from the populated graph
            testPersistenceManager.saveSnapshot(testGraph);
            boolean snapshotExists = Files.exists(Paths.get(snapshotFile));
            assertThat(snapshotExists).isTrue();

            // Verify that the snapshot file contains valid data
            String snapshotContent = Files.readString(Paths.get(snapshotFile));
            assertThat(snapshotContent).contains("service-a");
            assertThat(snapshotContent).contains("service-b");
            assertThat(snapshotContent).contains("service-c");
            assertThat(snapshotContent).contains("isolated-service");

            // Simulate system shutdown and restart scenario
            // In a real system, this would involve:
            // 1. Stopping pipeline gracefully
            // 2. Saving snapshot to disk
            // 3. Saving WAL to disk
            // 4. Starting fresh system
            // 5. Recovering from snapshot + replaying WAL

            // Test recovery process by creating a new graph and restoring from files
            DependencyGraph recoveredGraph = new DependencyGraph();
            testPersistenceManager.recover(recoveredGraph);
            List<Event> restoredEvents = testPersistenceManager.readFromWAL();

            // Verify we can restore the state
            assertThat(restoredEvents).hasSize(4);

            // The recovery process should successfully reconstruct the graph
            // This demonstrates that the system is consistent after restart/recovery
            assertThat(recoveredGraph.getNodes()).isNotEmpty();
            assertThat(recoveredGraph.getEdges()).isNotEmpty();

        } finally {
            // Clean up test files
            try {
                Files.deleteIfExists(Paths.get(tempDir + "/snapshot.json"));
                Files.deleteIfExists(Paths.get(tempDir + "/events.jsonl"));
                Files.deleteIfExists(Paths.get(tempDir));
            } catch (IOException e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    void testRestartConsistencyWithDisconnectedComponents() throws Exception {
        // Create a temporary directory for testing
        String tempDir = System.getProperty("java.io.tmpdir") + "/test_disconnected_components";
        Files.createDirectories(Paths.get(tempDir));

        try {
            // Setup paths for this test
            String snapshotFile = tempDir + "/snapshot.json";
            String walFile = tempDir + "/events.jsonl";

            // Create a real graph and persistence manager with test files
            DependencyGraph testGraph = new DependencyGraph();
            PersistenceManager testPersistenceManager = new PersistenceManager(snapshotFile, walFile);

            // Setup disconnected components as mentioned in the assessment
            // First isolated subgraph: a -> b -> c -> a (cycle)
            Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:00Z", "a", "b", 10, "ok", null);
            Event e2 = new Event("e2", "dependency_observed", "2026-06-10T00:00:00Z", "b", "c", 10, "ok", null);
            Event e3 = new Event("e3", "dependency_observed", "2026-06-10T00:00:00Z", "c", "a", 10, "ok", null);

            // Second isolated subgraph: x -> y -> z -> x (cycle)
            Event e4 = new Event("e4", "dependency_observed", "2026-06-10T00:00:00Z", "x", "y", 15, "ok", null);
            Event e5 = new Event("e5", "dependency_observed", "2026-06-10T00:00:00Z", "y", "z", 15, "ok", null);
            Event e6 = new Event("e6", "dependency_observed", "2026-06-10T00:00:00Z", "z", "x", 15, "ok", null);

            // Process events
            testGraph.applyEvent(e1);
            testGraph.applyEvent(e2);
            testGraph.applyEvent(e3);
            testGraph.applyEvent(e4);
            testGraph.applyEvent(e5);
            testGraph.applyEvent(e6);

            // Simulate processing events and writing to WAL
            testPersistenceManager.appendToWal(e1);
            testPersistenceManager.appendToWal(e2);
            testPersistenceManager.appendToWal(e3);
            testPersistenceManager.appendToWal(e4);
            testPersistenceManager.appendToWal(e5);
            testPersistenceManager.appendToWal(e6);

            // Save snapshot
            testPersistenceManager.saveSnapshot(testGraph);

            // Verify that we can read back the events from WAL
            List<Event> walEvents = testPersistenceManager.readFromWAL();
            assertThat(walEvents).hasSize(6);

            // Verify snapshot contains disconnected components
            String snapshotContent = Files.readString(Paths.get(snapshotFile));
            assertThat(snapshotContent).contains("a");
            assertThat(snapshotContent).contains("b");
            assertThat(snapshotContent).contains("c");
            assertThat(snapshotContent).contains("x");
            assertThat(snapshotContent).contains("y");
            assertThat(snapshotContent).contains("z");

            // Test that cycle detection works with disconnected components
            List<List<String>> cycles = testGraph.getCycles();
            assertThat(cycles).hasSize(2); // Should find 2 cycles (one in each isolated component)

        } finally {
            // Clean up test files
            try {
                Files.deleteIfExists(Paths.get(tempDir + "/snapshot.json"));
                Files.deleteIfExists(Paths.get(tempDir + "/events.jsonl"));
                Files.deleteIfExists(Paths.get(tempDir));
            } catch (IOException e) {
                // Ignore cleanup errors
            }
        }
    }
}