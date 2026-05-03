package com.example.drm.job;

import com.example.drm.cluster.ClusterMembershipManager;
import com.example.drm.model.NodeStatus;

import java.util.Collection;
public class JobScheduler {
    private final ClusterMembershipManager membership;
    private JobAssignmentStrategy strategy;

    public JobScheduler(ClusterMembershipManager membership) {
        this.membership = membership;
        this.strategy = new LeastLoadedStrategy();
    }

    public void setStrategy(JobAssignmentStrategy strategy) {
        this.strategy = strategy;
    }

    public String schedule(String jobId) {
        Collection<NodeStatus> members = membership.getAllMembers().values();
        return strategy.selectNode(members, membership.getLocalNodeId());
    }
}