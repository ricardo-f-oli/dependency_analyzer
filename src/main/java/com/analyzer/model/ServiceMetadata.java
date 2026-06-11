package com.analyzer.model;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata associated with a service node in the graph.
 */
public class ServiceMetadata {
    private final String name;
    private final Map<String, String> attributes = new ConcurrentHashMap<>();
    private volatile Instant lastHeartbeat;

    public ServiceMetadata(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void updateAttributes(Map<String, String> newAttributes) {
        if (newAttributes != null) {
            attributes.putAll(newAttributes);
        }
    }

    public Instant getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Instant lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }
}
