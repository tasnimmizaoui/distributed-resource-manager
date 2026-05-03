
#  Distributed Resource Manager (DRM)

[![Java 21](https://img.shields.io/badge/Java-21-orange?logo=java)](https://openjdk.org/projects/jdk/21/)
[![Maven](https://img.shields.io/badge/Maven-3.9%2B-blue?logo=apache-maven)](https://maven.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-1.59-green?logo=google)](https://grpc.io/)
[![Protocol Buffers](https://img.shields.io/badge/Protobuf-3.24-9cf?logo=protocols-dot-io)](https://protobuf.dev/)

Un **mini-orchestrateur distribué** inspiré de Kubernetes, ZooKeeper et Consul, conçu dans un cadre académique pour illustrer les concepts fondamentaux des systèmes distribués. Il coordonne l'exécution de tâches sur un cluster de nœuds, avec élection de leader, tolérance aux pannes, réplication d'état et exclusion mutuelle.

---

##  Table des matières

- [Aperçu du système](#️-aperçu-du-système)
- [ Architecture](#️-architecture)
- [ Fonctionnalités](#-fonctionnalités)
- [ Dépendances](#-dépendances)
- [ Lancement rapide](#-lancement-rapide)
- [ Structure du projet](#-structure-du-projet)
- [ Configuration](#-configuration)
- [ Exemples](#-exemples)
- [ Dépannage](#-dépannage)
- [ Concepts clés](#-concepts-clés)
- [️ Améliorations futures](#️-améliorations-futures)

---

## Aperçu du système

```text
┌─────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ Client  │ ───► │   Node 1     │ ◄──► │   Node 2     │ ◄──► │   Node 3     │
│ (gRPC)  │      │   Leader     │      │  Follower    │      │  Follower    │
└─────────┘      │ Port: 50051  │      │ Port: 50052  │      │ Port: 50053  │
                 └──────────────┘      └──────────────┘      └──────────────┘
                         │                      │                      │
                         └──────────────────────┼──────────────────────┘
                                                │
                                         Cluster distribué

```
### Rôles

* **Client** : soumet des tâches au cluster via gRPC.
* **Leader** : coordonne l'ensemble du système.
* **Followers** : exécutent les tâches assignées.
* **Cluster** : maintient la cohérence et la disponibilité.

---

##  Architecture

### Composants principaux

| Composant                  | Rôle                    |
| -------------------------- | ----------------------- |
| `gRPC Server`              | Point d'entrée réseau   |
| `ClusterMembershipManager` | Gestion des nœuds       |
| `LeaderElectionService`    | Élection du leader      |
| `HeartbeatService`         | Détection des pannes    |
| `DistributedLockManager`   | Exclusion mutuelle      |
| `JobManager`               | Coordination des tâches |
| `JobScheduler`             | Répartition des jobs    |
| `JobExecutor`              | Exécution asynchrone    |
| `ReplicatedStateMachine`   | Réplication             |
| `WriteAheadLog`            | Persistance locale      |

---

### Flux d'exécution d'une tâche

```text
1. Client → SubmitJob → Nœud quelconque
2. Si follower → ForwardJob → Leader
3. Leader → AcquireLock
4. Leader → Sélection du meilleur nœud
5. Leader → ExecuteJob → Worker
6. Worker → Exécution (Virtual Thread)
7. Worker → Retour résultat
8. Leader → ReleaseLock
9. Leader → Réponse au client
```

---

### Services gRPC

| Service          | RPC              | Description            |
| ---------------- | ---------------- | ---------------------- |
| `ClientService`  | `SubmitJob`      | Soumission externe     |
| `ClusterService` | `Heartbeat`      | Vérification de vie    |
|                  | `RequestVote`    | Élection               |
|                  | `AnnounceLeader` | Notification du leader |
|                  | `ReplicateState` | Réplication            |
|                  | `ForwardJob`     | Redirection interne    |
|                  | `ExecuteJob`     | Exécution distante     |
|                  | `AcquireLock`    | Verrouillage           |
|                  | `ReleaseLock`    | Déverrouillage         |

---

## Fonctionnalités

* Élection automatique du leader (Bully Algorithm)
* Tolérance aux pannes
* Heartbeats distribués
* Répartition intelligente des tâches
* Exécution concurrente via Virtual Threads
* Réplication d'état
* Journalisation WAL
* Proxy transparent vers le leader
* Verrouillage distribué
* Architecture extensible

---

## Dépendances

| Technologie | Version | Usage             |
| ----------- | ------- | ----------------- |
| Java        | 21      | Langage principal |
| Maven       | 3.9+    | Build             |
| gRPC        | 1.59    | Communication RPC |
| Protobuf    | 3.24    | Sérialisation     |
| SLF4J       | 2.0.9   | Logging           |
| Logback     | 1.4.11  | Backend de logs   |

---

## Lancement rapide

### Prérequis

```bash
java -version
mvn -version
```

---

### Compilation

```bash
git clone https://github.com/votre-username/distributed-resource-manager.git
cd distributed-resource-manager
mvn clean package
```

---

### Lancer le cluster

#### Node 1

```bash
mvn exec:java \
  -Dexec.mainClass="com.example.drm.bootstrap.DistributedNode" \
  -Dexec.args="node1 50051 node2:localhost:50052,node3:localhost:50053"
```

#### Node 2

```bash
mvn exec:java \
  -Dexec.mainClass="com.example.drm.bootstrap.DistributedNode" \
  -Dexec.args="node2 50052 node1:localhost:50051,node3:localhost:50053"
```

#### Node 3

```bash
mvn exec:java \
  -Dexec.mainClass="com.example.drm.bootstrap.DistributedNode" \
  -Dexec.args="node3 50053 node1:localhost:50051,node2:localhost:50052"
```

---

### Lancer le client

```bash
mvn exec:java \
  -Dexec.mainClass="com.example.drm.util.TestClient"
```

---

### Test de panne

1. Arrêter le leader (`Ctrl+C`)
2. Attendre quelques secondes
3. Observer la réélection automatique
4. Continuer à soumettre des jobs

---

##  Structure du projet

```text
distributed-resource-manager/
├── pom.xml
├── README.md
└── src/
    ├── main/
    │   ├── java/com/example/drm/
    │   │   ├── bootstrap/
    │   │   ├── cluster/
    │   │   ├── grpc/
    │   │   ├── job/
    │   │   ├── lock/
    │   │   ├── model/
    │   │   ├── state/
    │   │   └── util/
    │   ├── proto/
    │   └── resources/
    └── test/
```

---

##  Configuration

### Niveau de logs

```xml
<root level="INFO">
    <appender-ref ref="STDOUT"/>
</root>
```

---

### Paramètres Heartbeat

```java
private final long heartbeatIntervalMs = 2000;
private final long heartbeatTimeoutMs  = 6000;
```

---

### Stratégie de scheduling

```java
this.strategy = new LeastLoadedStrategy();
```

---

##  Exemples

### Soumettre un job

```java
ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()
        .build();

var stub = ClientServiceGrpc.newBlockingStub(channel);

JobDescriptor job = JobDescriptor.newBuilder()
        .setJobId(UUID.randomUUID().toString())
        .setJobType("demo")
        .setPayload("Hello World")
        .build();

SubmitJobResponse response = stub.submitJob(
        SubmitJobRequest.newBuilder()
                .setJob(job)
                .build()
);

System.out.println(response.getAssignedNode());
```

---

### Ajouter une nouvelle stratégie

```java
public class RoundRobinStrategy implements JobAssignmentStrategy {

    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public String selectNode(Collection<NodeStatus> members, String leaderId) {
        List<String> alive = members.stream()
                .filter(NodeStatus::isAlive)
                .map(n -> n.getEndpoint().nodeId())
                .toList();

        return alive.get(counter.getAndIncrement() % alive.size());
    }
}
```

---

##  Dépannage

| Problème                 | Solution               |
| ------------------------ | ---------------------- |
| `No leader available`    | Attendre l'élection    |
| `Connection refused`     | Vérifier les ports     |
| `ClassNotFoundException` | Exécuter `mvn compile` |
| `Port already in use`    | Changer le port        |
| Pas de réélection        | Vérifier les peers     |
| Dashboard inaccessible   | Vérifier `npm run dev` |

---

## Concepts clés

Ce projet illustre plusieurs piliers des systèmes distribués :

* Consensus
* Élection de leader
* Détection de pannes
* Réplication d'état
* Exclusion mutuelle
* Répartition de charge
* Persistance WAL
* Communication RPC
* Coordination distribuée

> En production, des solutions comme Raft, etcd ou ZooKeeper seraient préférables.

---


## Licence
Projet académique réalisé dans le cadre d'un cours de systèmes distribués.

Libre d'utilisation à des fins éducatives.

---

<div align="center">
  <sub>Built with Java 21, gRPC and distributed systems concepts.</sub>
</div>

