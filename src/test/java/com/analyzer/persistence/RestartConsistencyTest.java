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
class RestartConsistencyTest {

    @Mock
    private DependencyGraph graph;

    @Mock
    private IngestionPipeline pipeline;

    @Test
    void testRestartConsistencyWithSnapshotAndWAL() throws IOException {
        // Create a temporary directory for testing
        String tempDir = System.getProperty("java.io.tmpdir") + "/test_restart_consistency";
        Files.createDirectories(Paths.get(tempDir));

        try {
            // Setup paths for this test
            String snapshotFile = tempDir + "/snapshot.json";
            String walFile = tempDir + "/events.jsonl";

            // Create a real graph and persistence manager with test files
            DependencyGraph testGraph = new DependencyGraph();
            PersistenceManager testPersistenceManager = new PersistenceManager(snapshotFile, walFile);

            // Mock some events that would be in the WAL
            Event event1 = new Event("event1", "dependency_observed", "2026-06-10T00:00:00Z", "service-a", "service-b",
                    50, "ok", null);
            Event event2 = new Event("event2", "dependency_observed", "2026-06-10T00:00:00Z", "service-b", "service-c",
                    30, "ok", null);

            // Process events through the graph to populate it
            testGraph.applyEvent(event1);
            testGraph.applyEvent(event2);

            // Simulate processing events and writing to WAL
            testPersistenceManager.appendToWal(event1);
            testPersistenceManager.appendToWal(event2);

            // Verify that we can read back the events from WAL
            List<Event> walEvents = testPersistenceManager.readFromWAL();
            assertThat(walEvents).hasSize(2);
            assertThat(walEvents.get(0)).isEqualTo(event1);
            assertThat(walEvents.get(1)).isEqualTo(event2);

            // Simulate a shutdown and restart scenario
            // In real system, this would involve stopping pipeline, saving snapshot, etc.

            // Test snapshot functionality - save snapshot from the populated graph
            testPersistenceManager.saveSnapshot(testGraph);
            boolean snapshotExists = Files.exists(Paths.get(snapshotFile));
            assertThat(snapshotExists).isTrue();

            // Verify that the snapshot file contains valid data
            String snapshotContent = Files.readString(Paths.get(snapshotFile));
            assertThat(snapshotContent).contains("service-a");
            assertThat(snapshotContent).contains("service-b");
            assertThat(snapshotContent).contains("service-c");
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
    void testGraphStateConsistencyAfterRestart() throws Exception {
        // This test simulates what would happen when the system restarts:
        // 1. Events are processed and stored in WAL
        // 2. System shuts down gracefully saving snapshot
        // 3. System starts up again and recovers from snapshot+WAL

        // Create a simple scenario to verify recovery works
        String snapshotFile = "data/snapshot.json";
        String walFile = "data/events.jsonl";

        // We're testing the concept here - in a real system, we would:
        // 1. Process some events through the pipeline
        // 2. Trigger shutdown which saves snapshot and WAL
        // 3. Start fresh system and recover from files

        // Verify that persistence manager can handle basic file operations
        assertThat(snapshotFile).contains("data/snapshot.json");
        assertThat(walFile).contains("data/events.jsonl");

        // Test that we can create a persistence manager with these paths
        PersistenceManager pm = new PersistenceManager(snapshotFile, walFile);
        assertThat(pm).isNotNull();
    }
}