package com.example.drm.util;

import com.example.drm.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Random;
import java.util.UUID;

public class TestClient {
    public static void main(String[] args) throws Exception {
        // Liste des nœuds connus (host:port)
        String[][] nodes = {
                {"localhost", "50051"},
                {"localhost", "50052"},
                {"localhost", "50053"}
        };

        Random rand = new Random();

        // Envoyer quelques tâches
        for (int i = 0; i < 5; i++) {
            // Choisir un nœud au hasard
            int idx = rand.nextInt(nodes.length);
            String host = nodes[idx][0];
            int port = Integer.parseInt(nodes[idx][1]);

            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            ClientServiceGrpc.ClientServiceBlockingStub stub = ClientServiceGrpc.newBlockingStub(channel);

            JobDescriptor job = JobDescriptor.newBuilder()
                    .setJobId(UUID.randomUUID().toString())
                    .setJobType("demo")
                    .setPayload("Process-" + i)
                    .build();
            SubmitJobRequest request = SubmitJobRequest.newBuilder().setJob(job).build();

            System.out.println("Sending job to " + host + ":" + port);
            SubmitJobResponse resp = stub.submitJob(request);
            System.out.println("Response: success=" + resp.getSuccess()
                    + ", assigned_node=" + resp.getAssignedNode()
                    + ", message=" + resp.getMessage());

            channel.shutdown();
            Thread.sleep(1000);
        }
    }
}