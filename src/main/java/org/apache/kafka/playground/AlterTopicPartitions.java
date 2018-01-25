/**
 * Copyright 2017 Paolo Patierno
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.ù
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.kafka.playground;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AlterTopicPartitions {

    public static void main(String[] args) throws Exception {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);

        // create the topic with 1 partition
        NewTopic topic = new NewTopic("my_topic", 1, (short)1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));
        createTopicsResult.all().get();

        // get information about the topic, should have 1 partition
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton("my_topic"));
        TopicDescription topicDescription = describeTopicsResult.values().get("my_topic").get();
        System.out.println("partitions = " + topicDescription.partitions().size());

        // increase the number of partitions by 1 (up to 2 partitions)
        NewPartitions newPartitions = NewPartitions.increaseTo(2);
        Map<String, NewPartitions> map = new HashMap<>(1);
        map.put("my_topic", newPartitions);

        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(map);
        createPartitionsResult.all().get();

        // get information about the topic, should have 2 partition now
        describeTopicsResult = adminClient.describeTopics(Collections.singleton("my_topic"));
        topicDescription = describeTopicsResult.values().get("my_topic").get();
        System.out.println("partitions = " + topicDescription.partitions().size());

        adminClient.close();

        kafkaCluster.shutdown();
        dataDir.delete();
    }
}
