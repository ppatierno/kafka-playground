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

package org.apache.kafka.playground.streams.hello;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

public class HelloMain {

    public static void main(String[] args) {

        Topology topology = new Topology();

        topology
                .addSource("Source", "source-topic")
                .addProcessor("HelloProcessor", () -> new HelloProcessor(), "Source")
                /* without lambda with the ProcessorSupplier functional interface
                .addProcessor("HelloProcessor", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new HelloProcessor();
                    }
                }, "Source")
                */
                .addSink("Sink", "sink-topic", "HelloProcessor");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-stream-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        StreamsConfig config = new StreamsConfig(props);

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();
    }
}
