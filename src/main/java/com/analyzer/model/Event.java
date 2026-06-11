package com.analyzer.model;

import java.util.Map;

/**
 * Represents a dependency event emitted by a service.
 * Using a Java 21 Record for clean, immutable data holding.
 */
public record Event(
    String event_id,
    String type,          // "dependency_observed", "dependency_removed", "service_metadata", "heartbeat"
    String timestamp,
    String source,
    String target,
    Integer latency_ms,
    String status,        // "ok", "error", "timeout"
    Map<String, String> metadata
) {
    // Valid types
    public static final String TYPE_OBSERVED = "dependency_observed";
    public static final String TYPE_REMOVED = "dependency_removed";
    public static final String TYPE_METADATA = "service_metadata";
    public static final String TYPE_HEARTBEAT = "heartbeat";
}
