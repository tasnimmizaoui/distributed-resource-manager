package com.example.drm.bootstrap;

import com.example.drm.model.NodeEndpoint;
import java.util.Collections;
import java.util.Map;

public record NodeConfig(
        String nodeId,
        String host,
        int port,
        Map<String, NodeEndpoint> peerEndpoints
) {
    public NodeConfig {
        peerEndpoints = Collections.unmodifiableMap(peerEndpoints);
    }
}