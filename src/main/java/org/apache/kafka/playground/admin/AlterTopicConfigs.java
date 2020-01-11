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

package org.apache.kafka.playground.admin;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by ppatiern on 20/07/17.
 */
public class AlterTopicConfigs {

    public static void main(String[] args) throws Exception {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        kafkaCluster.createTopic("test", 1, 1);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "test");

        // get the current topic configuration
        DescribeConfigsResult describeConfigsResult  =
                adminClient.describeConfigs(Collections.singleton(resource));

        // print current retention.ms
        Map<ConfigResource, Config> config = describeConfigsResult.all().get();
        ConfigEntry retentionEntry =
                config.get(resource)
                .entries()
                .stream()
                .filter(entry -> entry.name().equals(TopicConfig.RETENTION_MS_CONFIG))
                .collect(Collectors.toList())
                .get(0);

        System.out.println(retentionEntry.value());

        // create a new entry for updating the retention.ms value on the same topic
        retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "51000");
        Map<ConfigResource, Collection<AlterConfigOp>> updateConfig = new HashMap<ConfigResource, Collection<AlterConfigOp>>();
        updateConfig.put(resource, Collections.singleton(new AlterConfigOp(retentionEntry, OpType.SET)));

        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(updateConfig);
        alterConfigsResult.all();

        describeConfigsResult  = adminClient.describeConfigs(Collections.singleton(resource));

        // print updated retention.ms
        config = describeConfigsResult.all().get();
        retentionEntry =
                config.get(resource)
                        .entries()
                        .stream()
                        .filter(entry -> entry.name().equals(TopicConfig.RETENTION_MS_CONFIG))
                        .collect(Collectors.toList())
                        .get(0);

        System.out.println(retentionEntry.value());

        adminClient.close();

        kafkaCluster.shutdown();
        dataDir.delete();
    }
}
