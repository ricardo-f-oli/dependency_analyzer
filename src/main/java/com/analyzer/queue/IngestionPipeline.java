package com.analyzer.queue;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.analyzer.persistence.PersistenceManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class IngestionPipeline {
    private static final Logger log = LoggerFactory.getLogger(IngestionPipeline.class);

    private final DependencyGraph graph;
    private final PersistenceManager persistenceManager;
    private final CustomBlockingQueue<Event> queue;
    private final int consumerCount;
    private final int producerCount;
    private final long snapshotIntervalEvents;

    private final List<Thread> consumerThreads = new ArrayList<>();
    private final List<Thread> producerThreads = Collections.synchronizedList(new ArrayList<>());
    private final ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor();
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong duplicateEvents = new AtomicLong(0);
    private final AtomicLong eventsPublished = new AtomicLong(0);
    private final ExecutorService producerExecutor;
    private volatile boolean shutdownInitiated = false;

    public IngestionPipeline(
            DependencyGraph graph,
            PersistenceManager persistenceManager,
            @Value("${app.queue.capacity}") int capacity,
            @Value("${app.queue.consumers}") int consumerCount,
            @Value("${app.queue.producers}") int producerCount,
            @Value("${app.persistence.snapshot-interval-events}") long snapshotIntervalEvents) {
        this.graph = graph;
        this.persistenceManager = persistenceManager;
        this.queue = new CustomBlockingQueue<>(capacity);
        this.consumerCount = consumerCount;
        this.producerCount = producerCount;
        this.snapshotIntervalEvents = snapshotIntervalEvents;
        this.producerExecutor = Executors.newFixedThreadPool(producerCount,
                runnable -> {
                    Thread t = new Thread(runnable);
                    t.setName("pipeline-producer-" + t);
                    t.setDaemon(true);
                    return t;
                });

        // Add explicit SIGTERM handler for robust shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> handleShutdown(), "SIGTERM-Handler"));
    }

    @PostConstruct
    public void start() {
        log.info("Recovering graph state before starting pipeline...");
        persistenceManager.recover(graph);

        log.info("Starting ingestion pipeline with {} consumers, {} producers configured...", consumerCount,
                producerCount);
        for (int i = 0; i < consumerCount; i++) {
            Thread t = new Thread(this::consumeLoop, "queue-consumer-" + i);
            t.start();
            consumerThreads.add(t);
        }
    }

    /**
     * Publishes a single event to the queue using multiple concurrent producers.
     * This ensures we have actual concurrent producers rather than just one thread.
     */
    public void publish(Event event) throws InterruptedException {
        // Submit to producer executor to ensure concurrent producer usage
        producerExecutor.submit(() -> {
            try {
                queue.put(event);
                eventsPublished.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Loads a dataset file using multiple concurrent producer threads.
     * Each producer is assigned a partition of the events and publishes them
     * to the shared blocking queue, demonstrating the multi-producer,
     * multi-consumer pipeline pattern required by the specification.
     *
     * Blocks until all producers have finished publishing.
     *
     * @param filePath Path to a JSONL file containing events
     * @return Summary of the load operation
     */
    public DatasetLoadResult loadDataset(String filePath) {
        log.info("Loading dataset from {} using {} producer threads...", filePath, producerCount);
        long startTime = System.currentTimeMillis();

        // 1. Read all events from file into memory
        List<Event> events = readEventsFromFile(filePath);
        if (events.isEmpty()) {
            log.warn("No events found in dataset file: {}", filePath);
            return new DatasetLoadResult(0, producerCount, 0, 0);
        }

        log.info("Parsed {} events from dataset. Distributing across {} producers...", events.size(), producerCount);

        // 2. Partition events across N producer threads for concurrent publishing
        int totalEvents = events.size();
        int chunkSize = (totalEvents + producerCount - 1) / producerCount;
        CountDownLatch latch = new CountDownLatch(producerCount);
        AtomicLong published = new AtomicLong(0);

        for (int i = 0; i < producerCount; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, totalEvents);
            if (start >= totalEvents) {
                // More producers configured than events available
                latch.countDown();
                continue;
            }
            List<Event> partition = events.subList(start, end);
            int producerId = i;

            Thread t = new Thread(() -> {
                log.info("Producer-{} started: publishing {} events (indices {}-{})",
                        producerId, partition.size(), start, end - 1);
                int count = 0;
                for (Event event : partition) {
                    try {
                        // Call appendToWal directly when publishing to ensure persistence is updated
                        persistenceManager.appendToWal(event);
                        queue.put(event);
                        eventsPublished.incrementAndGet();
                        published.incrementAndGet();
                        count++;
                    } catch (InterruptedException e) {
                        log.info("Producer-{} interrupted after publishing {} events", producerId, count);
                        Thread.currentThread().interrupt();
                        break;
                    } catch (IllegalStateException e) {
                        log.info("Producer-{} stopped (queue shutdown) after publishing {} events", producerId, count);
                        break;
                    }
                }
                log.info("Producer-{} finished: published {} events", producerId, count);
                latch.countDown();
            }, "dataset-producer-" + producerId);

            t.start();
            producerThreads.add(t);
        }

        // 3. Wait for all producers to finish publishing to the queue
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for producer threads");
            Thread.currentThread().interrupt();
        }

        long elapsed = System.currentTimeMillis() - startTime;
        long totalPublished = published.get();
        log.info("Dataset load complete: {} events published by {} producers in {} ms",
                totalPublished, producerCount, elapsed);

        return new DatasetLoadResult(totalEvents, producerCount, totalPublished, elapsed);
    }

    private List<Event> readEventsFromFile(String filePath) {
        List<Event> events = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File(filePath);

        if (!file.exists()) {
            log.error("Dataset file not found: {}", filePath);
            return events;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty())
                    continue;
                try {
                    events.add(mapper.readValue(line, Event.class));
                } catch (Exception e) {
                    log.warn("Skipping malformed event line: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("Failed to read dataset file: {}", filePath, e);
        }

        return events;
    }

    private void consumeLoop() {
        log.info("Consumer thread {} started", Thread.currentThread().getName());
        try {
            while (true) {
                Event event = queue.take();
                if (event == null) {
                    // Queue has been shut down and drained.
                    break;
                }

                try {
                    boolean applied = graph.applyEvent(event);
                    if (applied) {
                        persistenceManager.appendToWal(event);
                        long count = eventsProcessed.incrementAndGet();
                        if (count % snapshotIntervalEvents == 0) {
                            snapshotExecutor.submit(() -> persistenceManager.saveSnapshot(graph));
                        }
                    } else {
                        duplicateEvents.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Error applying event {}", event.event_id(), e);
                }
            }
        } catch (InterruptedException e) {
            log.info("Consumer thread {} interrupted, exiting", Thread.currentThread().getName());
            Thread.currentThread().interrupt();
        }
        log.info("Consumer thread {} stopped", Thread.currentThread().getName());
    }

    @PreDestroy
    public void shutdown() {
        log.info("Initiating graceful shutdown of ingestion pipeline...");

        // 1. Stop queue inputs — unblocks producers waiting on backpressure
        queue.shutdown();

        // 2. Wait for any active producer threads to stop
        log.info("Waiting for {} active producer threads to stop...", producerThreads.size());
        synchronized (producerThreads) {
            for (Thread t : producerThreads) {
                try {
                    t.join(3000);
                } catch (InterruptedException e) {
                    log.warn("Interrupted waiting for producer thread to finish");
                    Thread.currentThread().interrupt();
                }
            }
        }

        // 3. Wait for consumer threads to drain remaining items in queue
        log.info("Draining remaining {} events in the queue...", queue.size());
        for (Thread t : consumerThreads) {
            try {
                t.join(5000); // 5 second timeout per thread
            } catch (InterruptedException e) {
                log.warn("Interrupted waiting for consumer threads to finish");
                Thread.currentThread().interrupt();
            }
        }

        // 4. Shutdown snapshot executor
        snapshotExecutor.shutdown();
        try {
            if (!snapshotExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                snapshotExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            snapshotExecutor.shutdownNow();
        }

        // 5. Save final state snapshot
        log.info("Saving final graph snapshot...");
        persistenceManager.saveSnapshot(graph);
        log.info("Ingestion pipeline shutdown complete.");
    }

    private void handleShutdown() {
        if (shutdownInitiated) {
            return; // Prevent multiple executions
        }
        shutdownInitiated = true;

        log.info("Received SIGTERM signal, initiating graceful shutdown...");
        shutdown();
    }

    // --- Metrics ---
    public int getQueueDepth() {
        return queue.size();
    }

    public int getQueueCapacity() {
        return queue.capacity();
    }

    public long getEventsProcessed() {
        return eventsProcessed.get();
    }

    public long getDuplicateEvents() {
        return duplicateEvents.get();
    }

    public long getEventsPublished() {
        return eventsPublished.get();
    }

    public int getProducerCount() {
        return producerCount;
    }

    public int getConsumerCount() {
        return consumerCount;
    }

    // --- Data Transfer Record ---
    public record DatasetLoadResult(int totalEventsInFile, int producerThreads, long eventsPublished, long elapsedMs) {
    }
}