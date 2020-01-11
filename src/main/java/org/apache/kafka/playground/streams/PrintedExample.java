package org.apache.kafka.playground.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * PrintedExample
 */
public class PrintedExample {

    public static void main(String[] args) {
        
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> uppers = builder
                                            .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                                            .mapValues(v -> v.toUpperCase());

        uppers.to("upper", Produced.with(Serdes.String(), Serdes.String()));

        Printed<String, String> sysout = Printed.<String, String>toSysOut().withLabel("stdout");
        uppers.print(sysout);

        Topology topology = builder.build();

        System.out.println(topology.describe());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, PrintedExample.class.getSimpleName().replace("$", ""));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, PrintedExample.class.getSimpleName().replace("$", ""));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                streams.close();
            }
        });

        streams.start();
    }
}