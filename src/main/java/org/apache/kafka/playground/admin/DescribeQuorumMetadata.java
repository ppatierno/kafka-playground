package org.apache.kafka.playground.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.Node;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DescribeQuorumMetadata {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);

        // describing cluster to compare with what the quorum info provides in terms of brokers running
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        List<Integer> nodes = describeClusterResult.nodes().get().stream()
                .map(Node::id)
                .collect(Collectors.toList());
        System.out.println("Nodes = " + nodes);

        DescribeMetadataQuorumResult metadataQuorumResult = adminClient.describeMetadataQuorum();
        QuorumInfo quorumInfo = metadataQuorumResult.quorumInfo().get();
        System.out.println("Voters: " + getReplicasIds(quorumInfo.voters()));
        System.out.println("Observers: " + getReplicasIds(quorumInfo.observers()));

        adminClient.close();
    }

    private static List<Integer> getReplicasIds(List<QuorumInfo.ReplicaState> nodes) {
        return nodes.stream()
                .map(QuorumInfo.ReplicaState::replicaId)
                .collect(Collectors.toList());
    }
}
