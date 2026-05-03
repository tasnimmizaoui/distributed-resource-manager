package com.example.drm.job;

import com.example.drm.model.NodeStatus;

import java.util.Collection;
import java.util.Comparator;


public class LeastLoadedStrategy implements JobAssignmentStrategy {
    @Override
    public String selectNode(Collection<NodeStatus> liveMembers, String leaderId) {
        return liveMembers.stream()
                .filter(NodeStatus::isAlive)
                .min(Comparator.comparingInt(NodeStatus::getActiveJobs))
                .map(n -> n.getEndpoint().nodeId())
                .orElse(leaderId);
    }
}