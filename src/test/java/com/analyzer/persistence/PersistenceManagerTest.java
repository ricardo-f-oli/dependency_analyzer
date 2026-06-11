package com.analyzer.persistence;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PersistenceManagerTest {

    private DependencyGraph graph;
    private PersistenceManager manager;
    private File snapshotFile;
    private File walFile;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        graph = new DependencyGraph();
        snapshotFile = tempDir.resolve("snapshot.json").toFile();
        walFile = tempDir.resolve("events.jsonl").toFile();
        manager = new PersistenceManager(snapshotFile.getAbsolutePath(), walFile.getAbsolutePath());
    }

    @Test
    void testSnapshotAndWalRecovery() {
        // 1. Apply events to build initial graph
        Event e1 = new Event("e1", "dependency_observed", "2026-06-10T00:00:00Z", "A", "B", 10, "ok", null);
        Map<String, String> metaAttr = new HashMap<>();
        metaAttr.put("team", "ops");
        Event e2 = new Event("e2", "service_metadata", "2026-06-10T00:00:01Z", "A", null, null, null, metaAttr);

        graph.applyEvent(e1);
        graph.applyEvent(e2);

        // Save Snapshot
        manager.saveSnapshot(graph);
        assertThat(snapshotFile).exists();
        assertThat(walFile.length()).isEqualTo(0); // WAL must be truncated after snapshot

        // 2. Add post-snapshot events to WAL
        Event e3 = new Event("e3", "dependency_observed", "2026-06-10T00:00:05Z", "B", "C", 20, "ok", null);
        graph.applyEvent(e3);
        manager.appendToWal(e3);

        assertThat(walFile).exists();
        assertThat(walFile.length()).isGreaterThan(0);

        // 3. Create a fresh graph and recover it
        DependencyGraph freshGraph = new DependencyGraph();
        manager.recover(freshGraph);

        // Verify recovery details
        assertThat(freshGraph.getNodes()).containsKeys("A", "B", "C");
        assertThat(freshGraph.getNodes().get("A").getAttributes()).containsEntry("team", "ops");

        // Verify shortest path path
        DependencyGraph.ShortestPathResult path = freshGraph.getShortestPath("A", "C");
        assertThat(path).isNotNull();
        assertThat(path.path()).containsExactly("A", "B", "C");
        assertThat(path.totalWeight()).isEqualTo(30.0);
    }
}
