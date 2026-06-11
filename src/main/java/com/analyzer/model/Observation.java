package com.analyzer.model;

import java.time.Instant;

/**
 * Captures a single request's latency and status on a dependency edge.
 */
public record Observation(
    Instant timestamp,
    int latencyMs,
    String status // "ok", "error", "timeout"
) {}
