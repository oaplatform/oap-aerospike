package oap.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Peers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Executors;
import oap.concurrent.scheduler.ScheduledExecutorService;
import oap.io.Closeables;
import oap.util.Throwables;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * Created by igor.petrenko on 2020-09-16.
 */
@Slf4j
public class AerospikeCluster implements Closeable {
    private static final Method nodeBalanceConnectionsMethod;
    private static final Method clusterProcessRecoverQueueMethod;
    private static final Method nodeRefreshPeersMethod;
    private static final Method nodeRefreshPartitionsMethod;

    private static final Field NodeReferenceCountField;
    private static final Field nodePartitionChangedField;

    static {
        try {
            clusterProcessRecoverQueueMethod = Cluster.class.getDeclaredMethod("processRecoverQueue");
            clusterProcessRecoverQueueMethod.setAccessible(true);

            nodeBalanceConnectionsMethod = Node.class.getDeclaredMethod("balanceConnections");
            nodeBalanceConnectionsMethod.setAccessible(true);

            nodeRefreshPeersMethod = Node.class.getDeclaredMethod("refreshPeers", Peers.class);
            nodeRefreshPeersMethod.setAccessible(true);

            nodeRefreshPartitionsMethod = Node.class.getDeclaredMethod("refreshPartitions", Peers.class);
            nodeRefreshPartitionsMethod.setAccessible(true);

            NodeReferenceCountField = Node.class.getDeclaredField("referenceCount");
            NodeReferenceCountField.setAccessible(true);

            nodePartitionChangedField = Node.class.getDeclaredField("partitionChanged");
            nodePartitionChangedField.setAccessible(true);
        } catch (NoSuchMethodException | NoSuchFieldException e) {
            throw Throwables.propagate(e);
        }
    }

    private final AerospikeClient client;
    private ScheduledExecutorService scheduler;
    private int tendCount;

    public AerospikeCluster(boolean forceSingleNode, int tendInterval, AerospikeClient client) {
        if (forceSingleNode) {
            scheduler = Executors.newScheduledThreadPool(1, "aerospike-cluster");
            scheduler.scheduleWithFixedDelay(this::tend, 0, tendInterval, TimeUnit.MILLISECONDS);
        }
        this.client = client;
    }

    public void tend() {
        if (client == null) return;

        var cluster = client.getCluster();

        var nodes = cluster.getNodes();
        var peers = new Peers(nodes.length + 16, 16);

        for (var node : nodes) {
            setReferenceCount(node, 0);
            setPartitionChanged(node, false);
        }

        for (var node : nodes) {
            node.refresh(peers);
        }

        if (peers.genChanged) {
            // Refresh peers for all nodes that responded the first time even if only one node's peers changed.
            peers.refreshCount = 0;

            for (var node : nodes) {
                log.trace("refreshPeers...");
                refreshPeers(node, peers);
                log.trace("refreshPeers... Done");
            }
        }

        // Refresh partition map when necessary.
        for (var node : nodes) {
            if (isPartitionChanged(node)) {
                log.trace("refreshPartitions...");
                refreshPartitions(node, peers);
                log.trace("refreshPartitions... Done");
            }

            // Set partition maps for all namespaces to point to same node.
            for (var partitions : cluster.partitionMap.values()) {
                for (var nodeArray : partitions.replicas) {
                    int max = nodeArray.length();

                    for (var i = 0; i < max; i++) {
                        nodeArray.set(i, node);
                    }
                }
            }
        }

        // Balance connections every 30 tend intervals.
        if (++tendCount >= 30) {
            tendCount = 0;

            for (Node node : nodes) {
                log.trace("balanceConnections...");
                balanceConnections(node);
                log.trace("balanceConnections... Done");
            }

            if (cluster.eventState != null) {
                for (var es : cluster.eventState) {
                    var eventLoop = es.eventLoop;

                    eventLoop.execute(() -> {
                        for (var node : nodes) {
                            node.balanceAsyncConnections(eventLoop);
                        }
                    });
                }
            }
        }

        processRecoverQueue(cluster);
    }

    @SneakyThrows
    private void refreshPartitions(Node node, Peers peers) {
        nodeRefreshPartitionsMethod.invoke(node, peers);
    }

    @SneakyThrows
    private void setPartitionChanged(Node node, boolean value) {
        nodePartitionChangedField.set(node, value);
    }

    @SneakyThrows
    private boolean isPartitionChanged(Node node) {
        return (boolean) nodePartitionChangedField.get(node);
    }

    @SneakyThrows
    private void setReferenceCount(Node node, int value) {
        NodeReferenceCountField.set(node, value);
    }

    @SneakyThrows
    private void refreshPeers(Node node, Peers peers) {
        nodeRefreshPeersMethod.invoke(node, peers);
    }

    @SneakyThrows
    private Object processRecoverQueue(Cluster cluster) {
        return clusterProcessRecoverQueueMethod.invoke(cluster);
    }

    @SneakyThrows
    private void balanceConnections(Node node) {
        nodeBalanceConnectionsMethod.invoke(node);
    }

    @Override
    public void close() {
        Closeables.close(scheduler);
    }
}
