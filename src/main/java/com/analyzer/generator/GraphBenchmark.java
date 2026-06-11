package com.analyzer.generator;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmarks graph ingestion throughput and query latencies on the large dataset.
 */
public class GraphBenchmark {

    private static final String DATASET_FILE = "C:/Users/ricar/.gemini/antigravity-ide/scratch/dependency_analyzer/data/events_large.jsonl";

    public static void main(String[] args) {
        System.out.println("=== GRAPH PERFORMANCE BENCHMARK ===");
        File file = new File(DATASET_FILE);
        if (!file.exists()) {
            System.err.println("Dataset file not found! Please run DataGenerator first.");
            return;
        }

        // Load events into memory to avoid I/O bottlenecks during ingestion benchmark
        System.out.println("Loading dataset into memory...");
        List<Event> events = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        try (BufferedReader reader = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                events.add(mapper.readValue(line, Event.class));
            }
        } catch (IOException e) {
            System.err.println("Failed to read dataset file");
            e.printStackTrace();
            return;
        }
        System.out.println("Loaded " + events.size() + " events.");

        // 1. Benchmark Ingestion Speed
        System.out.println("\n--- Ingestion Benchmark ---");
        DependencyGraph graph = new DependencyGraph();
        long startIngestion = System.nanoTime();
        int appliedCount = 0;
        for (Event event : events) {
            if (graph.applyEvent(event)) {
                appliedCount++;
            }
        }
        long durationIngestion = System.nanoTime() - startIngestion;
        double durationMs = durationIngestion / 1_000_000.0;
        double throughput = events.size() / (durationIngestion / 1_000_000_000.0);

        System.out.printf("Total events processed: %d (applied unique: %d)%n", events.size(), appliedCount);
        System.out.printf("Time taken: %.2f ms%n", durationMs);
        System.out.printf("Ingestion throughput: %.2f events/sec%n", throughput);
        System.out.printf("Graph size: %d services, %d active edges%n", 
            graph.getNodes().size(), 
            graph.getEdges().values().stream().mapToInt(java.util.Map::size).sum());

        // 2. Benchmark Queries
        System.out.println("\n--- Query Latency Benchmark ---");
        int runs = 100;

        // Reachable
        benchmarkQuery("Reachable (blast radius)", runs, () -> {
            graph.getReachable("service-50");
        });

        // Dependents
        benchmarkQuery("Dependents (blast radius)", runs, () -> {
            graph.getDependents("database-service");
        });

        // Shortest Path
        benchmarkQuery("Shortest Path (Dijkstra)", runs, () -> {
            graph.getShortestPath("service-0", "database-service");
        });

        // Cycles
        benchmarkQuery("Cycle Detection", runs, () -> {
            graph.getCycles();
        });

        // Critical Services
        benchmarkQuery("Critical Services (k=10)", 10, () -> {
            graph.getCriticalServices(10);
        });

        // Health
        benchmarkQuery("Health (window=300s)", runs, () -> {
            graph.getHealth("auth-service", 300);
        });
    }

    private static void benchmarkQuery(String label, int runs, Runnable queryTask) {
        // Warmup
        for (int i = 0; i < 5; i++) {
            queryTask.run();
        }

        List<Double> latenciesMs = new ArrayList<>(runs);
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            queryTask.run();
            long duration = System.nanoTime() - start;
            latenciesMs.add(duration / 1_000_000.0);
        }

        double min = latenciesMs.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        double max = latenciesMs.stream().mapToDouble(Double::doubleValue).max().orElse(0);
        double avg = latenciesMs.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        java.util.Collections.sort(latenciesMs);
        double p95 = latenciesMs.get((int) Math.ceil(0.95 * runs) - 1);

        System.out.printf("%-25s | Min: %6.3f ms | Avg: %6.3f ms | P95: %6.3f ms | Max: %6.3f ms%n", 
            label, min, avg, p95, max);
    }
}
