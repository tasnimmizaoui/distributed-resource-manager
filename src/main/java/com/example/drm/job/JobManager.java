package com.example.drm.job;

import com.example.drm.model.NodeEndpoint;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.drm.bootstrap.NodeContext;
import com.example.drm.bootstrap.NodeConfig;
import com.example.drm.model.NodeStatus;
import com.example.drm.grpc.SubmitJobRequest;
import com.example.drm.grpc.SubmitJobResponse;
import com.example.drm.grpc.ExecuteJobRequest;
import com.example.drm.grpc.ExecuteJobResponse;
import com.example.drm.grpc.LockRequest;
import com.example.drm.grpc.LockResponse;
import com.example.drm.grpc.UnlockRequest;
import com.example.drm.grpc.NodeId;


public class JobManager {
    private static final Logger log = LoggerFactory.getLogger(JobManager.class);
    private final NodeContext context;

    public JobManager(NodeContext context) {
        this.context = context;
    }

    public void handleSubmitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        if (!context.getElectionService().isLeader()) {
            String leader = context.getElectionService().getCurrentLeader();
            if (leader == null) {
                responseObserver.onNext(SubmitJobResponse.newBuilder()
                        .setSuccess(false).setMessage("No leader available").build());
                responseObserver.onCompleted();
                return;
            }
            forwardToLeader(request, responseObserver);
        } else {
            processAsLeader(request, responseObserver);
        }
    }

    private void forwardToLeader(SubmitJobRequest request, StreamObserver<SubmitJobResponse> clientResp) {
        String leaderId = context.getElectionService().getCurrentLeader();
        NodeEndpoint leaderEp = context.getMembershipManager().getMember(leaderId).getEndpoint();
        try {
            SubmitJobResponse resp = context.getStubPool().getBlockingStub(leaderEp).forwardJob(request);
            clientResp.onNext(resp);
        } catch (Exception e) {
            clientResp.onNext(SubmitJobResponse.newBuilder()
                    .setSuccess(false).setMessage("Failed to forward to leader").build());
        }
        clientResp.onCompleted();
    }

    private void processAsLeader(SubmitJobRequest request, StreamObserver<SubmitJobResponse> clientResp) {
        // Vérifier le verrou distribué via le LockManager (leader)
        LockRequest lockReq = LockRequest.newBuilder()
                .setResourceId(request.getJob().getJobId())
                .setRequester(toProto(context.getConfig()))
                .setTimeoutMs(5000)
                .build();
        LockResponse lockResp = context.getLockManager().acquire(lockReq);
        if (!lockResp.getGranted()) {
            clientResp.onNext(SubmitJobResponse.newBuilder()
                    .setSuccess(false).setMessage("Job already running or locked").build());
            clientResp.onCompleted();
            return;
        }

        // Planifier via le scheduler
        String targetNode = context.getJobScheduler().schedule(request.getJob().getJobId());
        NodeEndpoint targetEp = context.getMembershipManager().getMember(targetNode).getEndpoint();
        NodeStatus targetStatus = context.getMembershipManager().getMember(targetNode);
        targetStatus.incrementJobs();

        // Envoyer la tâche au nœud cible pour exécution
        ExecuteJobRequest execReq = ExecuteJobRequest.newBuilder().setJob(request.getJob()).build();
        try {
            ExecuteJobResponse execResp = context.getStubPool().getBlockingStub(targetEp).executeJob(execReq);
            clientResp.onNext(SubmitJobResponse.newBuilder()
                    .setSuccess(execResp.getSuccess())
                    .setAssignedNode(targetNode)
                    .setMessage(execResp.getResult())
                    .build());
        } catch (Exception e) {
            targetStatus.decrementJobs();
            clientResp.onNext(SubmitJobResponse.newBuilder().setSuccess(false).setMessage("Execution failed").build());
        } finally {
            context.getLockManager().release(UnlockRequest.newBuilder()
                    .setResourceId(request.getJob().getJobId())
                    .setLockToken(lockResp.getLockToken())
                    .build());
        }
        clientResp.onCompleted();
    }

    // Méthode appelée lors de l'exécution locale d'une tâche (sur le nœud exécuteur)
    public void handleExecuteJob(ExecuteJobRequest request, StreamObserver<ExecuteJobResponse> resp) {
        context.getJobExecutor().execute(request, () -> {
                    String myId = context.getConfig().nodeId();
                    NodeStatus myStatus = context.getMembershipManager().getMember(myId);
                    if (myStatus != null) myStatus.decrementJobs();
                }).thenAccept(resp::onNext)
                .exceptionally(ex -> {
                    resp.onNext(ExecuteJobResponse.newBuilder().setSuccess(false).setResult(ex.getMessage()).build());
                    return null;
                })
                .thenRun(resp::onCompleted);
    }

    public void onBecomeLeader() { log.info("JobManager ready as leader"); }
    public void onLoseLeader() { log.info("JobManager no longer leader"); }

    private NodeId toProto(NodeConfig cfg) {
        return NodeId.newBuilder().setId(cfg.nodeId()).setHost(cfg.host()).setPort(cfg.port()).build();
    }
}