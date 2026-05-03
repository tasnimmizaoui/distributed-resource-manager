package com.example.drm.state;

import com.example.drm.grpc.*;
import com.example.drm.model.NodeEndpoint;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import com.example.drm.bootstrap.NodeContext;
import com.example.drm.bootstrap.NodeConfig;
import com.example.drm.model.NodeStatus;

public class ReplicatedStateMachine {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedStateMachine.class);
    private final NodeContext context;
    private final Map<String, String> state = new ConcurrentHashMap<>();
    private long sequence = 0;

    public ReplicatedStateMachine(NodeContext context) {
        this.context = context;
    }

    public synchronized void applyLocal(String key, String value) {
        sequence++;
        state.put(key, value);
        // Persistance WAL
        context.getWal().append(sequence, key, value);
        // Réplication
        replicateToFollowers(key, value, sequence);
    }

    private void replicateToFollowers(String key, String value, long seq) {
        if (!context.getElectionService().isLeader()) return;
        List<NodeEndpoint> followers = getFollowers();
        if (followers.isEmpty()) return;

        StateEntry entry = StateEntry.newBuilder()
                .setKey(key).setValue(value).setSequenceNumber(seq).build();
        ReplicateRequest request = ReplicateRequest.newBuilder()
                .addEntries(entry)
                .setLeader(toProto(context.getConfig()))
                .build();

        for (NodeEndpoint follower : followers) {
            try {
                ReplicateResponse resp = context.getStubPool().getBlockingStub(follower)
                        .replicateState(request);
                if (!resp.getSuccess()) {
                    log.warn("Replication to {} failed", follower.nodeId());
                }
            } catch (Exception e) {
                log.error("Replication error to {}", follower.nodeId(), e);
            }
        }
    }

    public void handleReplicateRequest(ReplicateRequest request, StreamObserver<ReplicateResponse> resp) {
        try {
            for (StateEntry entry : request.getEntriesList()) {
                state.put(entry.getKey(), entry.getValue());
                context.getWal().append(entry.getSequenceNumber(), entry.getKey(), entry.getValue());
                sequence = Math.max(sequence, entry.getSequenceNumber());
            }
            resp.onNext(ReplicateResponse.newBuilder().setSuccess(true).setAppliedSequence(sequence).build());
        } catch (Exception e) {
            resp.onNext(ReplicateResponse.newBuilder().setSuccess(false).build());
        }
        resp.onCompleted();
    }

    public String getState(String key) {
        return state.get(key);
    }

    private List<NodeEndpoint> getFollowers() {
        String local = context.getConfig().nodeId();
        return context.getMembershipManager().getAllMembers().values().stream()
                .filter(s -> !s.getEndpoint().nodeId().equals(local) && s.isAlive())
                .map(NodeStatus::getEndpoint)
                .toList();
    }

    private NodeId toProto(NodeConfig cfg) {
        return NodeId.newBuilder().setId(cfg.nodeId()).setHost(cfg.host()).setPort(cfg.port()).build();
    }
}