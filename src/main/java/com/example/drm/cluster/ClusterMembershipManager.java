package com.example.drm.cluster;

import com.example.drm.model.NodeEndpoint;
import com.example.drm.model.NodeStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterMembershipManager {
    private final Map<String, NodeStatus> members = new ConcurrentHashMap<>();
    private final String localNodeId;

    public ClusterMembershipManager(String localNodeId, Map<String, NodeEndpoint> initialPeers) {
        this.localNodeId = localNodeId;
        initialPeers.forEach((id, endpoint) -> members.put(id, new NodeStatus(endpoint)));
    }

    public NodeStatus getMember(String id) {
        return members.get(id);
    }

    public Map<String, NodeStatus> getAllMembers() {
        return members;
    }

    public String getLocalNodeId() {
        return localNodeId;
    }

    public void markAlive(String id) {
        NodeStatus status = members.get(id);
        if (status != null) {
            status.heartbeatReceived();
        }
    }

    public void markDead(String id) {
        NodeStatus status = members.get(id);
        if (status != null) {
            status.setAlive(false);
        }
    }

    public String findSmallestAliveNode() {
        return members.entrySet().stream()
                .filter(e -> e.getValue().isAlive())
                .map(Map.Entry::getKey)
                .min(String::compareTo)
                .orElse(null);
    }
}