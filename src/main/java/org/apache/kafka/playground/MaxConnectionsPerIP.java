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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MaxConnectionsPerIP {

    private AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        Properties config = new Properties();
        config.setProperty("max.connections.per.ip", "1");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .withKafkaConfiguration(config)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        new MaxConnectionsPerIP().run();

        kafkaCluster.shutdown();
        dataDir.delete();
    }

    private void run() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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

        try {

            consumer.subscribe(Collections.singleton("test"));
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                System.out.println("records = " + records.count());
            }

        } catch (WakeupException we) {
            // Ignore exception if closing
            if (running.get()) throw we;
        } finally {
            consumer.close();
        }
    }
}
