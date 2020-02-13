package org.apache.kafka.playground.consumer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.playground.common.StreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;

/**
 * ConsumerGroupOnTopics
 */
public class ConsumerGroupOnTopics {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOnTopics.class);

    private StreamConsumer<String, String> consumer1;
    private StreamConsumer<String, String> consumer2;
    private StreamConsumer<String, String> consumer3;

    public static void main(String[] args) throws IOException {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        kafkaCluster.createTopic("topic-1", 1, 1);
        kafkaCluster.createTopic("topic-2", 1, 1);
        kafkaCluster.createTopic("topic-3", 1, 1);
        
        ConsumerGroupOnTopics app = new ConsumerGroupOnTopics();
        app.run();

        System.in.read();

        app.stop();

        kafkaCluster.shutdown();
        dataDir.delete();
    }

    private void run() throws IOException {

        List<String> topics = Arrays.asList("topic-1", "topic-2", "topic-3");
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // default partition assignment strategy is RangeAssignor: with this assignor, only consumer 1 will get all
        // three topics and related partitions, consumer 2 and 3 will be idle (as usual behaviour)
        // switching to RoundRobinAssignor allows to rebalance partitions across consumers in the same consumer group across 
        // multiple topics (3 in this case) as they were all partitions of the same topic
        
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        this.consumer1 = new StreamConsumer<>(consumerProps);

        this.consumer1.start(topics, records -> {

            for (ConsumerRecord<String, String> record: records) {

                log.info("Consumer 1: record key = {} with value = {} on topic = {} partition = {}",
                        record.key(), record.value(), record.topic(), record.partition());
            }
        });

        this.consumer2 = new StreamConsumer<>(consumerProps);

        this.consumer2.start(topics, records -> {

            for (ConsumerRecord<String, String> record: records) {

                log.info("Consumer 2: record key = {} with value = {} on topic = {} partition = {}",
                        record.key(), record.value(), record.topic(), record.partition());
            }
        });

        this.consumer3 = new StreamConsumer<>(consumerProps);

        this.consumer3.start(topics, records -> {

            for (ConsumerRecord<String, String> record: records) {

                log.info("Consumer 3: record key = {} with value = {} on topic = {} partition = {}",
                        record.key(), record.value(), record.topic(), record.partition());
            }
        });
    }

    private void stop() {
        this.consumer1.stop();
        this.consumer2.stop();
        this.consumer3.stop();
    }
}