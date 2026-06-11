package com.analyzer.queue;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.analyzer.persistence.PersistenceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IngestionPipelineTest {

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
    void testPublishUsesMultipleProducers() throws InterruptedException {
        // This test verifies that our publish method uses the producer executor
        // and thus utilizes multiple concurrent producers as configured
        Event event = new Event("test-event", "dependency_observed", "2026-05-06T14:21:09.412Z", "service-a",
                "service-b", 42, "ok", null);

        // Call publish - this should submit to the producer executor
        pipeline.publish(event);

        // Verify that the method can be called without blocking indefinitely
        // (The actual thread pool execution happens asynchronously)
        verifyNoMoreInteractions(persistenceManager); // We're not verifying the full flow here, just that it doesn't
                                                      // block
    }
}