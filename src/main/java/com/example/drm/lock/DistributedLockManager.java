package com.example.drm.lock;

import com.example.drm.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class DistributedLockManager {
    private static final Logger log = LoggerFactory.getLogger(DistributedLockManager.class);
    private final Map<String, LockInfo> locks = new ConcurrentHashMap<>();
    private volatile boolean isLeader = false;

    public LockResponse acquire(LockRequest request) {
        if (!isLeader) {
            return LockResponse.newBuilder().setGranted(false).setLockToken("").build();
        }
        String resource = request.getResourceId();
        LockInfo existing = locks.get(resource);
        if (existing != null && !existing.isExpired()) {
            log.debug("Lock denied for {}", resource);
            return LockResponse.newBuilder().setGranted(false).setLockToken(existing.token()).build();
        }
        String token = UUID.randomUUID().toString();
        locks.put(resource, new LockInfo(token, System.currentTimeMillis() + request.getTimeoutMs()));
        log.debug("Lock granted for {} token {}", resource, token);
        return LockResponse.newBuilder().setGranted(true).setLockToken(token).build();
    }

    public UnlockResponse release(UnlockRequest request) {
        if (!isLeader) {
            return UnlockResponse.newBuilder().setSuccess(false).build();
        }
        LockInfo info = locks.get(request.getResourceId());
        if (info != null && info.token().equals(request.getLockToken())) {
            locks.remove(request.getResourceId());
            log.debug("Lock released for {}", request.getResourceId());
            return UnlockResponse.newBuilder().setSuccess(true).build();
        }
        return UnlockResponse.newBuilder().setSuccess(false).build();
    }

    public void onBecomeLeader() {
        isLeader = true;
        locks.clear(); // Nouveau leader, pas de verrou hérité (simplification)
    }

    public void onLoseLeader() {
        isLeader = false;
    }

    private record LockInfo(String token, long expireTime) {
        boolean isExpired() { return System.currentTimeMillis() > expireTime; }
    }
}