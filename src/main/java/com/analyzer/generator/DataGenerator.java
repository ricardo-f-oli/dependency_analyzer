package com.analyzer.generator;

import com.analyzer.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/**
 * Generates a realistic large synthetic dataset for testing.
 * Outputs a JSONL file containing ~100,000 events and ~6,000 services.
 */
public class DataGenerator {

    private static final int SERVICE_COUNT = 6000;
    private static final int EVENT_COUNT = 100000;
    private static final String OUTPUT_FILE = "C:/Users/ricar/.gemini/antigravity-ide/scratch/dependency_analyzer/data/events_large.jsonl";

    private static final String[] TEAMS = {"billing", "checkout", "auth", "inventory", "shipping", "frontend", "search", "recommendations"};
    private static final String[] TIERS = {"1", "2", "3", "4"};
    private static final String[] REGIONS = {"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"};
    private static final String[] STATUSES = {"ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "error", "timeout"};

    public static void main(String[] args) {
        System.out.println("Starting synthetic dataset generation...");
        long start = System.currentTimeMillis();

        List<Event> events = new ArrayList<>(EVENT_COUNT);
        Random rand = new Random(42); // fixed seed for repeatability

        // 1. Pre-define services
        List<String> services = new ArrayList<>();
        for (int i = 0; i < SERVICE_COUNT; i++) {
            services.add("service-" + i);
        }
        // Critical infrastructure hubs (high fan-in)
        String authService = "auth-service";
        String databaseService = "database-service";
        String paymentGateway = "payment-gateway";

        services.add(authService);
        services.add(databaseService);
        services.add(paymentGateway);

        // Map to keep track of active dependencies
        Set<String> activeEdges = new HashSet<>();

        // Generate events chronologically
        Instant baseTime = Instant.parse("2026-06-10T00:00:00Z");

        for (int i = 0; i < EVENT_COUNT; i++) {
            Instant eventTime = baseTime.plusSeconds(i);
            String eventId = "e-" + UUID.nameUUIDFromBytes((i + "-" + eventTime.toString()).getBytes());

            // Type distribution:
            // 70% observed, 10% removed, 10% metadata, 10% heartbeat
            double roll = rand.nextDouble();
            if (roll < 0.70) {
                // dependency_observed
                String src = services.get(rand.nextInt(services.size()));
                String tgt;

                // High fan-in probability
                double hubRoll = rand.nextDouble();
                if (hubRoll < 0.25) {
                    tgt = databaseService;
                } else if (hubRoll < 0.45) {
                    tgt = authService;
                } else if (hubRoll < 0.55) {
                    tgt = paymentGateway;
                } else {
                    // Standard DAG or random connection
                    int srcIdx = services.indexOf(src);
                    if (srcIdx < SERVICE_COUNT - 10) {
                        // Call a downstream service to encourage DAG hierarchy
                        tgt = services.get(srcIdx + 1 + rand.nextInt(5));
                    } else {
                        tgt = services.get(rand.nextInt(services.size()));
                    }
                }

                // Inject cycles explicitly for testing
                if (i % 5000 == 0) {
                    // Create explicit 3-node cycle: service-100 -> service-101 -> service-102 -> service-100
                    events.add(new Event("c1-" + i, "dependency_observed", eventTime.toString(), "service-100", "service-101", 12, "ok", null));
                    events.add(new Event("c2-" + i, "dependency_observed", eventTime.plusMillis(10).toString(), "service-101", "service-102", 15, "ok", null));
                    events.add(new Event("c3-" + i, "dependency_observed", eventTime.plusMillis(20).toString(), "service-102", "service-100", 8, "ok", null));
                    activeEdges.add("service-100->service-101");
                    activeEdges.add("service-101->service-102");
                    activeEdges.add("service-102->service-100");
                }

                if (src.equals(tgt)) {
                    tgt = databaseService; // avoid self-loops
                }

                activeEdges.add(src + "->" + tgt);

                // Latency distribution: log-normal shape using random gaussian
                // median 15ms, with a long tail (up to 3000ms+)
                int latency = (int) Math.exp(2.5 + rand.nextGaussian() * 0.8);
                if (latency < 1) latency = 1;
                if (latency > 5000) latency = 5000;

                String status = STATUSES[rand.nextInt(STATUSES.length)];

                events.add(new Event(eventId, Event.TYPE_OBSERVED, eventTime.toString(), src, tgt, latency, status, null));

            } else if (roll < 0.80) {
                // dependency_removed
                if (activeEdges.isEmpty()) {
                    i--; // retry
                    continue;
                }
                // Pick a random active edge to remove
                String edge = activeEdges.iterator().next();
                activeEdges.remove(edge);
                String[] parts = edge.split("->");

                events.add(new Event(eventId, Event.TYPE_REMOVED, eventTime.toString(), parts[0], parts[1], null, null, null));

            } else if (roll < 0.90) {
                // service_metadata
                String service = services.get(rand.nextInt(services.size()));
                Map<String, String> metadata = new HashMap<>();
                metadata.put("team", TEAMS[rand.nextInt(TEAMS.length)]);
                metadata.put("tier", TIERS[rand.nextInt(TIERS.length)]);
                metadata.put("region", REGIONS[rand.nextInt(REGIONS.length)]);

                events.add(new Event(eventId, Event.TYPE_METADATA, eventTime.toString(), service, null, null, null, metadata));

            } else {
                // heartbeat
                String service = services.get(rand.nextInt(services.size()));
                events.add(new Event(eventId, Event.TYPE_HEARTBEAT, eventTime.toString(), service, null, null, null, null));
            }
        }

        // 2. Shuffle a small fraction (e.g. 5%) of events to simulate out-of-order delivery
        System.out.println("Simulating out-of-order latency...");
        for (int i = 0; i < events.size(); i++) {
            if (rand.nextDouble() < 0.05) {
                int swapIdx = rand.nextInt(events.size());
                Event temp = events.get(i);
                events.set(i, events.get(swapIdx));
                events.set(swapIdx, temp);
            }
        }

        // 3. Duplicate a small fraction (e.g. 5%) of events to simulate duplicate packets
        System.out.println("Simulating duplicate events...");
        List<Event> duplicates = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            if (rand.nextDouble() < 0.05) {
                duplicates.add(events.get(i));
            }
        }
        events.addAll(duplicates);
        Collections.shuffle(events, rand);

        // 4. Write to JSONL file
        System.out.println("Writing dataset to file: " + OUTPUT_FILE);
        File file = new File(OUTPUT_FILE);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        ObjectMapper mapper = new ObjectMapper();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8))) {
            for (Event e : events) {
                writer.write(mapper.writeValueAsString(e));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Failed to write synthetic dataset to file");
            e.printStackTrace();
            return;
        }

        long end = System.currentTimeMillis();
        System.out.println("Synthetic dataset generation completed successfully!");
        System.out.println("Total Events Generated: " + events.size());
        System.out.println("Total Services Represented: ~" + (SERVICE_COUNT + 3));
        System.out.println("Output File Size: " + (file.length() / (1024 * 1024)) + " MB");
        System.out.println("Time taken: " + (end - start) + " ms");
    }
}
