package com.example.drm.bootstrap;

import com.example.drm.cluster.*;
import com.example.drm.lock.*;
import com.example.drm.job.*;
import com.example.drm.state.*;
import com.example.drm.grpc.*;
import io.grpc.ManagedChannel;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class NodeContext {
    private final NodeConfig config;
    private ClusterMembershipManager membershipManager;
    private HeartbeatService heartbeatService;
    private LeaderElectionService electionService;
    private DistributedLockManager lockManager;
    private JobManager jobManager;
    private JobExecutor jobExecutor;
    private ReplicatedStateMachine stateMachine;
    private WriteAheadLog wal;
    private Map<String, ManagedChannel> peerChannels;
    private ClusterServiceStubPool stubPool;
    private ExecutorService jobExecutorPool;
    private JobScheduler jobScheduler;

    public NodeContext(NodeConfig config) {
        this.config = config;
    }

    // Getters
    public NodeConfig getConfig() { return config; }
    public ClusterMembershipManager getMembershipManager() { return membershipManager; }
    public HeartbeatService getHeartbeatService() { return heartbeatService; }
    public LeaderElectionService getElectionService() { return electionService; }
    public DistributedLockManager getLockManager() { return lockManager; }
    public JobManager getJobManager() { return jobManager; }
    public JobExecutor getJobExecutor() { return jobExecutor; }
    public ReplicatedStateMachine getStateMachine() { return stateMachine; }
    public WriteAheadLog getWal() { return wal; }
    public Map<String, ManagedChannel> getPeerChannels() { return peerChannels; }
    public ClusterServiceStubPool getStubPool() { return stubPool; }
    public ExecutorService getJobExecutorPool() { return jobExecutorPool; }
    public JobScheduler getJobScheduler() { return jobScheduler; }

    // Setters (utilisés lors de l'assemblage du nœud)
    public void setMembershipManager(ClusterMembershipManager m) { this.membershipManager = m; }
    public void setHeartbeatService(HeartbeatService s) { this.heartbeatService = s; }
    public void setElectionService(LeaderElectionService s) { this.electionService = s; }
    public void setLockManager(DistributedLockManager m) { this.lockManager = m; }
    public void setJobManager(JobManager m) { this.jobManager = m; }
    public void setJobExecutor(JobExecutor e) { this.jobExecutor = e; }
    public void setStateMachine(ReplicatedStateMachine m) { this.stateMachine = m; }
    public void setWal(WriteAheadLog w) { this.wal = w; }
    public void setPeerChannels(Map<String, ManagedChannel> c) { this.peerChannels = c; }
    public void setStubPool(ClusterServiceStubPool p) { this.stubPool = p; }
    public void setJobExecutorPool(ExecutorService p) { this.jobExecutorPool = p; }
    public void setJobScheduler(JobScheduler s) { this.jobScheduler = s; }
}