package com.example.drm.job;

import java.util.Collection;
import com.example.drm.model.NodeStatus;
@FunctionalInterface
public interface JobAssignmentStrategy {
    String selectNode(Collection<NodeStatus> liveMembers, String leaderId);
}