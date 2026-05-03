package com.example.drm.grpc;

import com.example.drm.bootstrap.NodeContext;
import io.grpc.stub.StreamObserver;

public class ClientServiceImpl extends ClientServiceGrpc.ClientServiceImplBase {
    private final NodeContext context;

    public ClientServiceImpl(NodeContext context) {
        this.context = context;
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        context.getJobManager().handleSubmitJob(request, responseObserver);
    }
}