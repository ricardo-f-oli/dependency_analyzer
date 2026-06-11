package com.analyzer.model;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks performance and status telemetry for a directed dependency edge.
 */
public class EdgeMetrics {
    private final String source;
    private final String target;
    private final LinkedList<Observation> observations = new LinkedList<>();
    private final ReentrantLock lock = new ReentrantLock();

    // Max age of observations to retain in memory to prevent leaks (e.g. 1 hour)
    private static final Duration MAX_RETENTION = Duration.ofHours(1);

    public EdgeMetrics(String source, String target) {
        this.source = source;
        this.target = target;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    /**
     * Records an observation and trims expired records.
     */
    public void addObservation(Observation obs) {
        lock.lock();
        try {
            observations.add(obs);
            pruneOldObservations(obs.timestamp());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cleans up observations older than MAX_RETENTION.
     */
    private void pruneOldObservations(Instant now) {
        Instant threshold = now.minus(MAX_RETENTION);
        while (!observations.isEmpty() && observations.peekFirst().timestamp().isBefore(threshold)) {
            observations.removeFirst();
        }
    }

    /**
     * Calculates the rolling average latency using all currently stored
     * observations.
     * If no observations exist, returns a baseline of 1.0 ms.
     */
    public double getRollingAverageLatency() {
        lock.lock();
        try {
            if (observations.isEmpty()) {
                return 1.0; // fallback default weight
            }
            double sum = 0;
            for (Observation obs : observations) {
                sum += obs.latencyMs();
            }
            return sum / observations.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets a snapshot list of observations recorded after the cutoff timestamp.
     */
    public List<Observation> getObservationsSince(Instant cutoff) {
        lock.lock();
        try {
            List<Observation> result = new ArrayList<>();
            for (Observation obs : observations) {
                if (!obs.timestamp().isBefore(cutoff)) {
                    result.add(obs);
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Helper to clear observations during resets/replays.
     */
    public void clear() {
        lock.lock();
        try {
            observations.clear();
        } finally {
            lock.unlock();
        }
    }
}
