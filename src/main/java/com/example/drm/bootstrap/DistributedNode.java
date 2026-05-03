package com.example.drm.bootstrap;

import com.example.drm.cluster.*;
import com.example.drm.grpc.*;
import com.example.drm.job.*;
import com.example.drm.lock.*;
import com.example.drm.model.NodeEndpoint;
import com.example.drm.state.*;
import io.grpc.*;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class DistributedNode {
    private final NodeContext context;
    private Server grpcServer;

    public DistributedNode(NodeConfig config) {
        this.context = new NodeContext(config);
        buildComponents();
    }

    private void buildComponents() {
        NodeConfig config = context.getConfig();
        // Initialiser les stubs (à faire après démarrage serveur pour écouter)
        // Ordre : membership, WAL, state machine, election, lock, job executor, job scheduler, job manager, heartbeat

        WriteAheadLog wal = new WriteAheadLog(config.nodeId());
        context.setWal(wal);

        ClusterMembershipManager membership = new ClusterMembershipManager(config.nodeId(), config.peerEndpoints());
        context.setMembershipManager(membership);

        ReplicatedStateMachine stateMachine = new ReplicatedStateMachine(context);
        context.setStateMachine(stateMachine);

        // Les stubs seront injectés plus tard (après création du serveur et canaux)
        // Nous utilisons un stub pool temporaire
        ClusterServiceStubPool stubPool = new ClusterServiceStubPool();
        context.setStubPool(stubPool);

        DistributedLockManager lockManager = new DistributedLockManager();
        context.setLockManager(lockManager);

        JobExecutor jobExecutor = new JobExecutor();
        context.setJobExecutor(jobExecutor);
        context.setJobExecutorPool(Executors.newVirtualThreadPerTaskExecutor()); // alternative

        JobScheduler scheduler = new JobScheduler(membership);
        context.setJobScheduler(scheduler);

        JobManager jobManager = new JobManager(context);
        context.setJobManager(jobManager);

        LeaderElectionService election = new LeaderElectionService(context);
        context.setElectionService(election);

        HeartbeatService heartbeat = new HeartbeatService(context);
        context.setHeartbeatService(heartbeat);
    }

    public void start() throws IOException {
        // Créer et connecter les stubs vers les pairs
        Map<String, NodeEndpoint> peers = context.getConfig().peerEndpoints();
        ClusterServiceStubPool stubPool = context.getStubPool();
        for (NodeEndpoint ep : peers.values()) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(ep.host(), ep.port())
                    .usePlaintext()
                    .build();
            stubPool.addChannel(ep.nodeId(), channel);
        }

        // Démarrer le serveur gRPC
        Server server = ServerBuilder.forPort(context.getConfig().port())
                .addService(new ClientServiceImpl(context))
                .addService(new ClusterServiceImpl(context))
                .build();
        server.start();
        grpcServer = server;
        System.out.println("Node " + context.getConfig().nodeId() + " started on port " + context.getConfig().port());

        // Replay WAL
        context.getWal().replay((key, value) -> context.getStateMachine().getState(key));

        // Démarrer les services de fond
        context.getHeartbeatService().start();
        context.getElectionService().start();

        // Ajouter un shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            stop();
        }));
    }

    public void stop() {
        if (grpcServer != null) grpcServer.shutdown();
        context.getJobExecutor().shutdown();
        context.getStubPool().shutdownAll();
    }

    public static void main(String[] args) throws Exception {
        // Usage: <nodeId> <port> <peerList>
        // peerList: id1:host1:port1,id2:...
        if (args.length < 3) {
            System.out.println("Args: nodeId port peerList");
            System.exit(1);
        }
        String id = args[0];
        int port = Integer.parseInt(args[1]);
        Map<String, NodeEndpoint> peers = parsePeers(args[2]);
        NodeConfig config = new NodeConfig(id, "localhost", port, peers);
        new DistributedNode(config).start();
        // Bloquer le main thread
        Thread.currentThread().join();
    }

    private static Map<String, NodeEndpoint> parsePeers(String list) {
        Map<String, NodeEndpoint> map = new java.util.HashMap<>();
        if (list.isEmpty()) return map;
        for (String token : list.split(",")) {
            String[] parts = token.split(":");
            if (parts.length == 3) {
                map.put(parts[0], new NodeEndpoint(parts[0], parts[1], Integer.parseInt(parts[2])));
            }
        }
        return map;
    }
}