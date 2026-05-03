package com.example.drm.job;

import com.example.drm.grpc.ExecuteJobRequest;
import com.example.drm.grpc.ExecuteJobResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public CompletableFuture<ExecuteJobResponse> execute(ExecuteJobRequest request, Runnable onComplete) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("Executing job {}", request.getJob().getJobId());
            try {
                Thread.sleep(1000); // simulation de traitement
            } catch (InterruptedException ignored) {}
            log.info("Job {} completed", request.getJob().getJobId());
            onComplete.run();
            return ExecuteJobResponse.newBuilder().setSuccess(true).setResult("OK").build();
        }, executor);
    }

    public void shutdown() {
        executor.shutdown();
    }
}