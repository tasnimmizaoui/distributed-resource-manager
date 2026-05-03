package com.example.drm.grpc;

import com.example.drm.bootstrap.NodeContext;
import io.grpc.stub.StreamObserver;

public class ClusterServiceImpl extends ClusterServiceGrpc.ClusterServiceImplBase {
    private final NodeContext context;

    public ClusterServiceImpl(NodeContext context) {
        this.context = context;
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> resp) {
        context.getMembershipManager().markAlive(request.getSender().getId());
        resp.onNext(HeartbeatResponse.newBuilder().setAcknowledged(true).setLeaderTimestamp(0).build());
        resp.onCompleted();
    }

    @Override
    public void requestVote(ElectionRequest request, StreamObserver<ElectionResponse> resp) {
        context.getElectionService().handleVoteRequest(request, resp);
    }

    @Override
    public void announceLeader(LeaderAnnouncement request, StreamObserver<Ack> resp) {
        context.getElectionService().handleAnnouncement(request);
        resp.onNext(Ack.newBuilder().build());
        resp.onCompleted();
    }

    @Override
    public void replicateState(ReplicateRequest request, StreamObserver<ReplicateResponse> resp) {
        context.getStateMachine().handleReplicateRequest(request, resp);
    }

    @Override
    public void forwardJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> resp) {
        // appelé sur le leader, traiter comme soumission directe
        context.getJobManager().handleSubmitJob(request, resp);
    }

    @Override
    public void executeJob(ExecuteJobRequest request, StreamObserver<ExecuteJobResponse> resp) {
        context.getJobManager().handleExecuteJob(request, resp);
    }

    @Override
    public void acquireLock(LockRequest request, StreamObserver<LockResponse> resp) {
        LockResponse response = context.getLockManager().acquire(request);
        resp.onNext(response);
        resp.onCompleted();
    }

    @Override
    public void releaseLock(UnlockRequest request, StreamObserver<UnlockResponse> resp) {
        UnlockResponse response = context.getLockManager().release(request);
        resp.onNext(response);
        resp.onCompleted();
    }
}