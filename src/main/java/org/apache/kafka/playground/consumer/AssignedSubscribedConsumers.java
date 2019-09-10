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

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ppatiern on 26/07/17.
 */
public class AssignedSubscribedConsumers {

    private AtomicBoolean consuming = new AtomicBoolean(true);
    private ExecutorService executorService;

    public void start() {

        this.executorService = Executors.newFixedThreadPool(2);
        this.executorService.submit(new AssignedConsumer());
        this.executorService.submit(new SubscribedConsumer());
    }

    public void stop() throws InterruptedException {

        this.consuming.set(false);
        this.executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        this.executorService.shutdownNow();
    }

    public static void main(String[] args) throws Exception {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        AssignedSubscribedConsumers consumers = new AssignedSubscribedConsumers();
        consumers.start();
        System.in.read();
        consumers.stop();

        kafkaCluster.shutdown();
        dataDir.delete();
    }

    private class AssignedConsumer implements Runnable {

        @Override
        public void run() {

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = null;

            try {
                consumer = new KafkaConsumer<>(props);
                consumer.assign(Collections.singleton(new TopicPartition("test", 0)));

                while (consuming.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("AssignedConsumer: record value = " + record.value() +
                                " on topic = " + record.topic() +
                                " partition = " + record.partition());
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
    }

    private class SubscribedConsumer implements Runnable {

        @Override
        public void run() {

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = null;
            try {

                consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singleton("test"), new ConsumerRebalanceListener() {

                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        if (collection.size() > 0) {
                            System.out.println("Revoked partitions ...");
                            for (TopicPartition topicPartition : collection) {
                                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                            }
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        if (collection.size() > 0) {
                            System.out.println("Assigned partitions ...");
                            for (TopicPartition topicPartition : collection) {
                                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                            }
                        }
                    }
                });

                while (consuming.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("SubscribedConsumer: record value = " + record.value() +
                                " on topic = " + record.topic() +
                                " partition = " + record.partition());
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
    }
}
