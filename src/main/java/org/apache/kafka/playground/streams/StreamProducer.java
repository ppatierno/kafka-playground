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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Represents a producer for a stream
 *
 * @param <K>   message key type
 * @param <V>   message value type
 */
public class StreamProducer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(StreamProducer.class);

    private static final int AWAIT_TERMINATION_TIMEOUT = 1000; // ms

    private AtomicBoolean running = new AtomicBoolean(true);

    private Properties props;
    private KafkaProducer<K, V> producer;

    private Function<Void, ProducerRecord<K, V>> generator;
    private Callback callback;
    private long delay;
    private ExecutorService executorService;

    /**
     * Constructor
     *
     * @param props configuration for the internal Kafka producer
     * @param delay delay for sending periodic messages to the stream
     */
    public StreamProducer(Properties props, long delay) {

        this.props = props;
        this.delay = delay;
        this.executorService = Executors.newSingleThreadExecutor();

        this.producer = new KafkaProducer<>(this.props);
    }

    /**
     * Start the stream producer on a different thread
     *
     * @param generator function for generating records to send
     * @param callback  callback called when the message is acked
     */
    public void start(Function<Void, ProducerRecord<K, V>> generator, Callback callback) {

        Objects.requireNonNull(generator);

        log.info("Starting the producer ...");
        this.generator = generator;
        this.callback = callback;
        this.executorService.execute(() -> {

            log.info("... started");
            try {
                while (running.get()) {

                    ProducerRecord<K, V> record = this.generator.apply(null);
                    producer.send(record, this.callback);
                    log.info("Sent record " + record);
                    Thread.sleep(delay);
                }

            } catch (InterruptedException ex) {
                producer.close();
            }

        });
    }

    /**
     * Start the stream producer on a different thread
     *
     * @param generator function for generating records to send
     */
    public void start(Function<Void, ProducerRecord<K, V>> generator) {
        this.start(generator, null);
    }

    /**
     * Stop the stream producer
     */
    public void stop() {

        log.info("Stopping the producer ...");
        this.running.set(false);
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(StreamProducer.AWAIT_TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
            log.info("... stopped");
        } catch (InterruptedException e) {
            log.error("Thread interrupted while awaiting executor termination");
        }
    }

}
