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
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.File;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopic {

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

        NewTopic topic = new NewTopic("my_topic", 2, (short)1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));

        try {
            createTopicsResult.all().get();
        // real failure cause is wrapped inside the raised ExecutionException
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.err.println("Topic already exists !!");
            } else if (e.getCause() instanceof TimeoutException) {
                System.err.println("Timeout !!");
            }
            e.printStackTrace();
        } finally {
            adminClient.close();

            kafkaCluster.shutdown();
            dataDir.delete();
        }
    }
}
