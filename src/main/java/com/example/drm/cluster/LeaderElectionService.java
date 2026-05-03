package com.example.drm.cluster;

import com.example.drm.bootstrap.NodeContext;
import com.example.drm.grpc.*;
import com.example.drm.model.NodeStatus;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import com.example.drm.bootstrap.NodeConfig;
public class LeaderElectionService {
    private static final Logger log = LoggerFactory.getLogger(LeaderElectionService.class);
    private final NodeContext context;
    private final AtomicReference<String> currentLeader = new AtomicReference<>(null);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public LeaderElectionService(NodeContext context) {
        this.context = context;
    }

    public void start() {
        // Déclencher une élection au démarrage
        startElection();
    }

    public void onNodeFailure(String failedNodeId) {
        if (currentLeader.get() != null && currentLeader.get().equals(failedNodeId)) {
            log.info("Leader {} failed, starting election", failedNodeId);
            startElection();
        }
    }

    public void startElection() {
        if (!electionInProgress.compareAndSet(false, true)) return;
        executor.submit(() -> {
            try {
                performElection();
            } finally {
                electionInProgress.set(false);
            }
        });
    }

    private void performElection() {
        String myId = context.getConfig().nodeId();
        ClusterMembershipManager membership = context.getMembershipManager();
        ClusterServiceStubPool stubs = context.getStubPool();
        boolean foundSmaller = false;
        for (NodeStatus status : membership.getAllMembers().values()) {
            String peerId = status.getEndpoint().nodeId();
            if (peerId.equals(myId) || !status.isAlive()) continue;
            if (peerId.compareTo(myId) < 0) {
                foundSmaller = true;
                try {
                    ElectionRequest request = ElectionRequest.newBuilder()
                            .setCandidate(toProto(context.getConfig()))
                            .build();
                    ElectionResponse response = stubs.getBlockingStub(status.getEndpoint())
                            .requestVote(request);
                    if (response.getOk()) {
                        // Un plus petit a répondu, il devient leader potentiel
                        log.debug("Smaller node {} responded OK, aborting election", peerId);
                        return;
                    }
                } catch (Exception e) {
                    log.warn("Failed to contact {} during election", peerId);
                }
            }
        }
        if (!foundSmaller) {
            // Je suis le plus petit vivant → je deviens leader
            becomeLeader();
        }
    }

    private void becomeLeader() {
        String myId = context.getConfig().nodeId();
        log.info("Becoming leader: {}", myId);
        currentLeader.set(myId);
        // Annoncer à tous les autres
        LeaderAnnouncement announcement = LeaderAnnouncement.newBuilder()
                .setLeader(toProto(context.getConfig()))
                .setElectionTerm(System.currentTimeMillis())
                .build();
        for (NodeStatus status : context.getMembershipManager().getAllMembers().values()) {
            if (status.getEndpoint().nodeId().equals(myId)) continue;
            try {
                context.getStubPool().getBlockingStub(status.getEndpoint())
                        .announceLeader(announcement);
            } catch (Exception e) {
                log.warn("Could not announce leadership to {}", status.getEndpoint().nodeId());
            }
        }
        context.getJobManager().onBecomeLeader();
        context.getLockManager().onBecomeLeader();
    }

    public String getCurrentLeader() {
        return currentLeader.get();
    }

    public boolean isLeader() {
        return context.getConfig().nodeId().equals(currentLeader.get());
    }

    public void handleVoteRequest(ElectionRequest request, StreamObserver<ElectionResponse> resp) {
        String candidateId = request.getCandidate().getId();
        String myId = context.getConfig().nodeId();
        if (candidateId.compareTo(myId) < 0 && context.getMembershipManager().getMember(candidateId).isAlive()) {
            resp.onNext(ElectionResponse.newBuilder().setOk(true).setResponder(toProto(context.getConfig())).build());
        } else {
            resp.onNext(ElectionResponse.newBuilder().setOk(false).setResponder(toProto(context.getConfig())).build());
        }
        resp.onCompleted();
    }

    public void handleAnnouncement(LeaderAnnouncement announcement) {
        String leaderId = announcement.getLeader().getId();
        log.info("New leader announced: {}", leaderId);
        currentLeader.set(leaderId);
        // Si on est follower, on réinitialise les rôles
        if (!isLeader()) {
            context.getJobManager().onLoseLeader();
            context.getLockManager().onLoseLeader();
        }
    }

    private NodeId toProto(NodeConfig cfg) {
        return NodeId.newBuilder().setId(cfg.nodeId()).setHost(cfg.host()).setPort(cfg.port()).build();
    }
}