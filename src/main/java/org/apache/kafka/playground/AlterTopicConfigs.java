package org.apache.kafka.playground;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

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
        Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
        updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(updateConfig);
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
    }
}
