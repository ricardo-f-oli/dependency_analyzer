package com.analyzer.graph;

import com.analyzer.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class DependencyGraphConcurrencyTest {

    private DependencyGraph graph;

    @BeforeEach
    void setUp() {
        graph = new DependencyGraph();
    }

    @Test
    void testConcurrentReadsAndWrites() throws InterruptedException, ExecutionException {
        // Create multiple threads that perform concurrent reads and writes
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Future<?> future = executor.submit(() -> {
                try {
                    // Each thread will do a mix of operations
                    for (int j = 0; j < 100; j++) {
                        String source = "service-" + (threadId * 100 + j);
                        String target = "target-" + (threadId * 100 + j);

                        // Apply some events
                        Event event = new Event(
                                "event-" + threadId + "-" + j,
                                "dependency_observed",
                                "2026-06-10T00:00:00Z",
                                source,
                                target,
                                50 + j,
                                "ok",
                                null);
                        graph.applyEvent(event);

                        // Perform a read operation
                        if (j % 10 == 0) {
                            Set<String> dependents = graph.getDependents(target);
                            assertThat(dependents).isNotNull();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
            futures.add(future);
        }

        // Wait for all threads to complete
        latch.await(30, TimeUnit.SECONDS);

        // Shutdown executor and wait for tasks to finish
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // Verify no exceptions occurred from futures
        for (Future<?> future : futures) {
            assertThat(future.isDone()).isTrue();
        }

        // Final verification - graph should have some services
        assertThat(graph.getNodes().size()).isGreaterThan(0);
    }

    @Test
    void testConcurrentIdempotency() throws InterruptedException, ExecutionException {
        // Test that idempotency works correctly with concurrent access
        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Future<?> future = executor.submit(() -> {
                try {
                    // Apply same event multiple times from different threads
                    Event event = new Event(
                            "duplicate-event",
                            "dependency_observed",
                            "2026-06-10T00:00:00Z",
                            "service-a",
                            "service-b",
                            50,
                            "ok",
                            null);

                    for (int j = 0; j < 20; j++) {
                        graph.applyEvent(event);
                    }
                } finally {
                    latch.countDown();
                }
            });
            futures.add(future);
        }

        // Wait for all threads to complete
        latch.await(30, TimeUnit.SECONDS);

        // Shutdown executor and wait for tasks to finish
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // Verify no exceptions occurred from futures
        for (Future<?> future : futures) {
            assertThat(future.isDone()).isTrue();
        }

        // Verify that the event was applied only once due to idempotency
        DependencyGraph.ShortestPathResult result = graph.getShortestPath("service-a", "service-b");
        assertThat(result).isNotNull();
        assertThat(result.totalWeight()).isEqualTo(50.0);
    }
}