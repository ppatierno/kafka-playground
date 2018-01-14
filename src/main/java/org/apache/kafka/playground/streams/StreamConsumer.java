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

package org.apache.kafka.playground.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Represents a consumer for a stream
 *
 * @param <K>   message key type
 * @param <V>   message value type
 */
public class StreamConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

    private static final int AWAIT_TERMINATION_TIMEOUT = 1000; // ms

    private AtomicBoolean running = new AtomicBoolean(true);

    private Properties props;
    private KafkaConsumer<K, V> consumer;

    private ExecutorService executorService;

    /**
     * Constructor
     *
     * @param props configuration for the internal Kafka consumer
     */
    public StreamConsumer(Properties props) {

        this.props = props;
        this.executorService = Executors.newSingleThreadExecutor();

        this.consumer = new KafkaConsumer<>(props);
    }

    /**
     * Start the stream consumer on a different thread
     *
     * @param topics    topics collection to subscribe
     * @param handler   handler for handling received records
     */
    public void start(Collection<String> topics, Consumer<ConsumerRecords<K, V>> handler) {

        log.info("Starting the consumer ...");

        this.consumer.subscribe(topics);
        this.executorService.execute(() -> {

            log.info("... started");
            try {
                while (running.get()) {

                    ConsumerRecords<K, V> records = consumer.poll(100);
                    handler.accept(records);
                }
            } catch (WakeupException we) {
                // Ignore exception if closing
                if (running.get()) throw we;
            } finally {
                consumer.close();
            }

        });
    }

    /**
     * Stop the stream consumer
     */
    public void stop() {

        log.info("Stopping the consumer ...");
        this.running.set(false);
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(StreamConsumer.AWAIT_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
            log.info("... stopped");
        } catch (InterruptedException e) {
            log.error("Thread interrupted while awaiting executor termination");
        }
    }
}
