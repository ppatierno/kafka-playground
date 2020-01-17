/**
 * Copyright 2018 Paolo Patierno
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

package org.apache.kafka.playground.streams.state;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.playground.common.StreamConsumer;
import org.apache.kafka.playground.common.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class StreamProducerConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(StreamProducerConsumerApp.class);

    private StreamProducer<String, Integer> producer1;
    private StreamProducer<String, Integer> producer2;
    private StreamConsumer<String, Integer> consumer;
    private int i = 0;

    public static void main(String[] args) throws IOException {

        File dataDir = Testing.Files.createTestingDirectory("cluster");

        KafkaCluster kafkaCluster = new KafkaCluster()
                .usingDirectory(dataDir)
                .withPorts(2181, 9092)
                .deleteDataPriorToStartup(true)
                .addBrokers(1)
                .startup();

        StreamProducerConsumerApp app = new StreamProducerConsumerApp();
        app.run();

        System.in.read();

        app.stop();

        kafkaCluster.shutdown();
        dataDir.delete();
    }

    private void run() {

        String consumerTopic = System.getenv("STREAM_CONSUMER_TOPIC");
        String producerTopic = System.getenv("STREAM_PRODUCER_TOPIC");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");

        this.consumer = new StreamConsumer<>(consumerProps);

        this.consumer.start(Collections.singleton(consumerTopic), records -> {

            for (ConsumerRecord<String, Integer> record: records) {

                log.info("Consumer: record key = {} with value = {} on topic = {} partition = {}",
                        record.key(), record.value(), record.topic(), record.partition());
            }
        });

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        this.producer1 = new StreamProducer<>(producerProps, 1000);
        this.producer2 = new StreamProducer<>(producerProps, 1000);

        this.producer1.start(v -> new ProducerRecord<>(producerTopic, "producer-1", 10));
        this.producer2.start(v -> new ProducerRecord<>(producerTopic, "producer-2", 20));
    }

    private void stop() {

        this.producer1.stop();
        this.producer2.stop();
        this.consumer.stop();
    }
}
