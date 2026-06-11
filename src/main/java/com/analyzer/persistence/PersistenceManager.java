package com.analyzer.persistence;

import com.analyzer.graph.DependencyGraph;
import com.analyzer.model.Event;
import com.analyzer.model.Observation;
import com.analyzer.model.ServiceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class PersistenceManager {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManager.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String snapshotPath;
    private final String walPath;
    private final ReentrantLock walLock = new ReentrantLock();

    public PersistenceManager(
            @Value("${app.persistence.snapshot-file}") String snapshotPath,
            @Value("${app.persistence.wal-file}") String walPath) {
        this.snapshotPath = snapshotPath;
        this.walPath = walPath;

        // Ensure directories exist
        ensureDirectoriesExist(snapshotPath);
        ensureDirectoriesExist(walPath);
    }

    private void ensureDirectoriesExist(String filePath) {
        File file = new File(filePath);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            if (parent.mkdirs()) {
                log.info("Created parent directory structure for persistence: {}", parent.getAbsolutePath());
            }
        }
    }

    /**
     * Appends an event to the Write-Ahead Log (WAL) on disk.
     */
    public void appendToWal(Event event) {
        walLock.lock();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(walPath, StandardCharsets.UTF_8, true))) {
            String json = objectMapper.writeValueAsString(event);
            writer.write(json);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.error("Failed to append event to WAL: {}", event.event_id(), e);
        } finally {
            walLock.unlock();
        }
    }

    /**
     * Saves a full snapshot of the graph and empties the WAL file.
     */
    public void saveSnapshot(DependencyGraph graph) {
        log.info("Starting graph snapshot to {}", snapshotPath);
        long startTime = System.currentTimeMillis();

        GraphSnapshotDto snapshotDto = buildSnapshotDto(graph);

        // Atomic write: write to temp file then rename
        File tempFile = new File(snapshotPath + ".tmp");
        File finalFile = new File(snapshotPath);

        try {
            objectMapper.writeValue(tempFile, snapshotDto);
            if (finalFile.exists() && !finalFile.delete()) {
                log.warn("Could not delete old snapshot file before replacing");
            }
            if (!tempFile.renameTo(finalFile)) {
                throw new IOException("Failed to rename temporary snapshot file to final file");
            }

            // Truncate the WAL
            clearWal();

            log.info("Graph snapshot completed in {} ms", System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            log.error("Failed to save snapshot", e);
        }
    }

    /**
     * Restores graph state from snapshot and replays subsequent WAL events.
     */
    public void recover(DependencyGraph graph) {
        log.info("Beginning graph recovery state...");
        graph.clear();

        File snapshotFile = new File(snapshotPath);
        if (snapshotFile.exists()) {
            try {
                log.info("Loading snapshot from {}", snapshotPath);
                GraphSnapshotDto snapshotDto = objectMapper.readValue(snapshotFile, GraphSnapshotDto.class);
                restoreSnapshot(graph, snapshotDto);
                log.info("Snapshot loaded. Restored {} nodes.", graph.getNodes().size());
            } catch (IOException e) {
                log.error("Failed to read snapshot file, skipping snapshot loading", e);
            }
        } else {
            log.info("No snapshot found at {}. Starting fresh.", snapshotPath);
        }

        File walFile = new File(walPath);
        if (walFile.exists()) {
            log.info("Replaying WAL events from {}", walPath);
            int count = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(walFile, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty())
                        continue;
                    try {
                        Event event = objectMapper.readValue(line, Event.class);
                        graph.applyEvent(event);
                        count++;
                    } catch (Exception e) {
                        log.warn("Failed to parse WAL line, skipping: {}", line, e);
                    }
                }
                log.info("WAL replay finished. Applied {} events.", count);
            } catch (IOException e) {
                log.error("Failed to read WAL file", e);
            }
        } else {
            log.info("No WAL file found at {}.", walPath);
        }
    }

    public void clearWal() {
        walLock.lock();
        try {
            File walFile = new File(walPath);
            if (walFile.exists()) {
                try (FileOutputStream fos = new FileOutputStream(walFile)) {
                    fos.getChannel().truncate(0); // clear content
                }
            }
        } catch (IOException e) {
            log.error("Failed to clear WAL file", e);
        } finally {
            walLock.unlock();
        }
    }

    /**
     * Reads all events from the Write-Ahead Log (WAL) file.
     */
    public List<Event> readFromWAL() {
        List<Event> events = new ArrayList<>();
        File walFile = new File(walPath);
        if (walFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(walPath, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty())
                        continue;
                    try {
                        Event event = objectMapper.readValue(line, Event.class);
                        events.add(event);
                    } catch (Exception e) {
                        log.warn("Failed to parse WAL line, skipping: {}", line, e);
                    }
                }
            } catch (IOException e) {
                log.error("Failed to read WAL file", e);
            }
        } else {
            log.info("WAL file does not exist at path: {}", walPath);
        }
        return events;
    }

    private GraphSnapshotDto buildSnapshotDto(DependencyGraph graph) {
        List<ServiceMetadataDto> nodesList = new ArrayList<>();
        graph.getNodes().forEach((name, meta) -> {
            String hb = meta.getLastHeartbeat() != null ? meta.getLastHeartbeat().toString() : null;
            nodesList.add(new ServiceMetadataDto(name, new HashMap<>(meta.getAttributes()), hb));
        });

        List<EdgeMetricsDto> edgesList = new ArrayList<>();
        graph.getEdges().forEach((src, targets) -> {
            targets.forEach((tgt, metrics) -> {
                List<ObservationDto> obsList = new ArrayList<>();
                metrics.getObservationsSince(Instant.MIN).forEach(obs -> {
                    obsList.add(new ObservationDto(obs.timestamp().toString(), obs.latencyMs(), obs.status()));
                });
                edgesList.add(new EdgeMetricsDto(src, tgt, obsList));
            });
        });

        Map<String, String> edgeLastUpdatedStr = new HashMap<>();
        graph.getEdgeLastUpdated().forEach((key, val) -> edgeLastUpdatedStr.put(key, val.toString()));

        return new GraphSnapshotDto(
                graph.getProcessedEventIds(),
                new ArrayList<>(), // This is intentionally left empty - it's not used in this implementation
                nodesList,
                edgesList,
                edgeLastUpdatedStr);
    }

    private void restoreSnapshot(DependencyGraph graph, GraphSnapshotDto dto) {
        // Restore processed event IDs
        if (dto.processedEventIds() != null) {
            for (String eventId : dto.processedEventIds()) {
                graph.addProcessedEventId(eventId);
            }
        }

        // Restore edge last updated timestamps
        if (dto.edgeLastUpdated() != null) {
            dto.edgeLastUpdated().forEach((key, timeStr) -> {
                graph.putEdgeLastUpdated(key, Instant.parse(timeStr));
            });
        }

        // Restores nodes
        if (dto.nodes() != null) {
            for (ServiceMetadataDto nodeDto : dto.nodes()) {
                ServiceMetadata meta = graph.getNodes().computeIfAbsent(nodeDto.name(), ServiceMetadata::new);
                meta.updateAttributes(nodeDto.attributes());
                if (nodeDto.lastHeartbeat() != null) {
                    meta.setLastHeartbeat(Instant.parse(nodeDto.lastHeartbeat()));
                }
            }
        }

        // Restores edges & observations
        if (dto.edges() != null) {
            for (EdgeMetricsDto edgeDto : dto.edges()) {
                List<Observation> observations = new ArrayList<>();
                for (ObservationDto obsDto : edgeDto.observations()) {
                    observations.add(new Observation(
                            Instant.parse(obsDto.timestamp()),
                            obsDto.latencyMs(),
                            obsDto.status()));
                }
                graph.restoreEdge(edgeDto.source(), edgeDto.target(), observations);
            }
        }
    }

    // --- DTO Records for Serialization ---
    public record ServiceMetadataDto(String name, Map<String, String> attributes, String lastHeartbeat) {
    }

    public record ObservationDto(String timestamp, int latencyMs, String status) {
    }

    public record EdgeMetricsDto(String source, String target, List<ObservationDto> observations) {
    }

    public record GraphSnapshotDto(
            Set<String> processedEventIds,
            List<String> lastProcessedIds,
            List<ServiceMetadataDto> nodes,
            List<EdgeMetricsDto> edges,
            Map<String, String> edgeLastUpdated) {
    }
}
