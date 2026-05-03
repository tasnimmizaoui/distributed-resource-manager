package com.example.drm.cluster;

import com.example.drm.grpc.ClusterServiceGrpc;
import com.example.drm.model.NodeEndpoint;
import io.grpc.ManagedChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterServiceStubPool {
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public void addChannel(String nodeId, ManagedChannel channel) {
        channels.put(nodeId, channel);
    }

    public ClusterServiceGrpc.ClusterServiceBlockingStub getBlockingStub(NodeEndpoint endpoint) {
        ManagedChannel ch = channels.get(endpoint.nodeId());
        if (ch == null) throw new IllegalStateException("No channel for " + endpoint.nodeId());
        return ClusterServiceGrpc.newBlockingStub(ch);
    }

    public ClusterServiceGrpc.ClusterServiceStub getAsyncStub(NodeEndpoint endpoint) {
        ManagedChannel ch = channels.get(endpoint.nodeId());
        return ClusterServiceGrpc.newStub(ch);
    }

    public void shutdownAll() {
        channels.values().forEach(ManagedChannel::shutdown);
    }
}