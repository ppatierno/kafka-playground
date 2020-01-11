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

package org.apache.kafka.playground.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class AddRemoveSubscriptions implements ConsumerRebalanceListener {

    private AtomicBoolean running = new AtomicBoolean(true);
    private List<String> topics;

    public static void main(String[] args) throws IOException {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        new AddRemoveSubscriptions().run();

        kafkaCluster.shutdown();
        dataDir.delete();
    }

    private void run() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                running.set(false);
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        topics = new ArrayList<>();

        try {

            consumer.subscribe(Collections.singleton("topics"), this);
            while (running.get()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumer: record value = " + record.value() +
                            " on topic = " + record.topic() +
                            " partition = " + record.partition());

                    if (record.value().startsWith("add:")) {
                        String topic = record.value().substring("add:".length());
                        // avoiding operation on "topics" and adding an already subscribed topic
                        if (!topic.equals("topics") && !topics.contains(topic)) {
                            List<String> topics = new ArrayList<>(this.topics);
                            topics.add(topic);
                            consumer.subscribe(topics, this);
                        }
                    } else if (record.value().startsWith("remove:")) {
                        String topic = record.value().substring("remove:".length());
                        // avoiding operation on "topics" and removing a not subscribed topic
                        if (!topic.equals("topics") && topics.contains(topic)) {
                            List<String> topics = new ArrayList<>(this.topics);
                            topics.remove(topic);
                            consumer.subscribe(topics, this);
                        }
                    }
                }
            }
        } catch (WakeupException we) {
            // Ignore exception if closing
            if (running.get()) throw we;
        } finally {
            consumer.close();
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        if (collection.size() > 0) {
            System.out.println("Revoked partitions ...");
            for (TopicPartition topicPartition : collection) {
                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                topics.remove(topicPartition.topic());
            }
            System.out.println("topics = " + topics);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        if (collection.size() > 0) {
            System.out.println("Assigned partitions ...");
            for (TopicPartition topicPartition : collection) {
                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                topics.add(topicPartition.topic());
            }
            System.out.println("topics = " + topics);
        }
    }
}
