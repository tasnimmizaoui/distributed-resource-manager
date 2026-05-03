package com.example.drm.cluster;

import com.example.drm.bootstrap.NodeConfig;
import com.example.drm.bootstrap.NodeContext;
import com.example.drm.grpc.*;
import com.example.drm.model.NodeEndpoint;
import com.example.drm.model.NodeStatus;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.Map;

public class HeartbeatService {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);
    private final NodeContext context;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final long heartbeatIntervalMs = 2000;
    private final long heartbeatTimeoutMs = 6000;

    public HeartbeatService(NodeContext context) {
        this.context = context;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::checkLiveness, heartbeatTimeoutMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        ClusterMembershipManager membership = context.getMembershipManager();
        ClusterServiceStubPool stubs = context.getStubPool();
        for (Map.Entry<String, NodeStatus> entry : membership.getAllMembers().entrySet()) {
            if (entry.getKey().equals(membership.getLocalNodeId())) continue;
            NodeEndpoint peer = entry.getValue().getEndpoint();
            HeartbeatRequest req = HeartbeatRequest.newBuilder()
                    .setSender(toProto( context.getConfig() ))
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            stubs.getAsyncStub(peer).heartbeat(req, new StreamObserver<HeartbeatResponse>() {
                public void onNext(HeartbeatResponse resp) {
                    membership.markAlive(entry.getKey());
                }
                public void onError(Throwable t) {
                    log.warn("Heartbeat failed to {}", peer.nodeId());
                }
                public void onCompleted() {}
            });
        }
    }

    private void checkLiveness() {
        long now = System.currentTimeMillis();
        ClusterMembershipManager membership = context.getMembershipManager();
        for (NodeStatus status : membership.getAllMembers().values()) {
            if (status.getEndpoint().nodeId().equals(membership.getLocalNodeId())) continue;
            if (status.isAlive() && (now - status.getLastHeartbeat()) > heartbeatTimeoutMs) {
                log.warn("Node {} seems dead", status.getEndpoint().nodeId());
                membership.markDead(status.getEndpoint().nodeId());
                context.getElectionService().onNodeFailure(status.getEndpoint().nodeId());
            }
        }
    }

    private NodeId toProto(NodeConfig cfg) {
        return NodeId.newBuilder().setId(cfg.nodeId()).setHost(cfg.host()).setPort(cfg.port()).build();
    }
}