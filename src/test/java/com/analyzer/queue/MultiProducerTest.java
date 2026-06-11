package com.analyzer.queue;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.analyzer.persistence.PersistenceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MultiProducerTest {

    @Mock
    private DependencyGraph graph;

    @Mock
    private PersistenceManager persistenceManager;

    private IngestionPipeline pipeline;

    @BeforeEach
    void setUp() {
        pipeline = new IngestionPipeline(
                graph,
                persistenceManager,
                1000, // capacity
                2, // consumers
                2, // producers
                10000 // snapshot interval
        );
    }

    @Test
    void testLoadDatasetUsesMultipleProducers() throws IOException, InterruptedException {
        // Create a temporary events file for testing
        String tempDir = System.getProperty("java.io.tmpdir");
        String testFile = tempDir + "/test_events.jsonl";

        // Create some test events
        String event1Json = "{\"event_id\":\"event1\",\"type\":\"dependency_observed\",\"timestamp\":\"2026-06-10T00:00:00Z\",\"source\":\"service-a\",\"target\":\"service-b\",\"latency_ms\":50,\"status\":\"ok\"}";
        String event2Json = "{\"event_id\":\"event2\",\"type\":\"dependency_observed\",\"timestamp\":\"2026-06-10T00:01:00Z\",\"source\":\"service-b\",\"target\":\"service-c\",\"latency_ms\":30,\"status\":\"ok\"}";
        String event3Json = "{\"event_id\":\"event3\",\"type\":\"dependency_observed\",\"timestamp\":\"2026-06-10T00:02:00Z\",\"source\":\"service-c\",\"target\":\"service-d\",\"latency_ms\":45,\"status\":\"ok\"}";

        Files.write(Paths.get(testFile), List.of(event1Json, event2Json, event3Json));

        try {
            // Test that loadDataset method can be called and uses multiple producers
            IngestionPipeline.DatasetLoadResult result = pipeline.loadDataset(testFile);

            // Verify the result shows correct producer count and events processed
            assertThat(result.producerThreads()).isEqualTo(2);
            assertThat(result.totalEventsInFile()).isEqualTo(3);
            assertThat(result.eventsPublished()).isEqualTo(3);

            // Verify that the persistence manager was called appropriately
            verify(persistenceManager, atLeastOnce()).appendToWal(any(Event.class));
        } finally {
            // Clean up test file
            Files.deleteIfExists(Paths.get(testFile));
        }
    }

    @Test
    void testPublishMethodUsesProducerExecutor() throws InterruptedException {
        // Create a test event - using the correct record constructor
        Event event = new Event("test-event", "dependency_observed", "2026-05-06T14:21:09.412Z", "service-a",
                "service-b", 42, "ok", null);

        // Call publish - this should submit to the producer executor
        pipeline.publish(event);

        // Verify that we can call publish without blocking indefinitely
        // (The actual thread pool execution happens asynchronously)
        verifyNoMoreInteractions(persistenceManager); // We're not verifying full flow here, just that it doesn't block
    }
}
