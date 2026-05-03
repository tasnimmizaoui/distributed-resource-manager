package com.example.drm.model;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeStatus {
    private final NodeEndpoint endpoint;
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger activeJobs = new AtomicInteger(0);

    public NodeStatus(NodeEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public NodeEndpoint getEndpoint() { return endpoint; }
    public boolean isAlive() { return alive.get(); }
    public void setAlive(boolean value) { alive.set(value); }
    public long getLastHeartbeat() { return lastHeartbeat.get(); }
    public void heartbeatReceived() {
        lastHeartbeat.set(System.currentTimeMillis());
        alive.set(true);
    }
    public int getActiveJobs() { return activeJobs.get(); }
    public void incrementJobs() { activeJobs.incrementAndGet(); }
    public void decrementJobs() { activeJobs.decrementAndGet(); }
}