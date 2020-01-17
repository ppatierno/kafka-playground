package org.apache.kafka.playground.streams.state;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * StateMain
 */
public class StateMain {

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Integer> source = builder.stream("source-topic",
                Consumed.with(Serdes.String(), Serdes.Integer()));

        KeyValueBytesStoreSupplier supplier =  Stores.inMemoryKeyValueStore("accumulatorStore");
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(supplier, Serdes.String(), Serdes.Integer());
        builder.addStateStore(storeBuilder);

        KStream<String, Integer> transformed = source.transformValues(() -> new AccumulatorTransformer(), "accumulatorStore");

        transformed.to("sink-topic", Produced.with(Serdes.String(), Serdes.Integer()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-stream-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                streams.close();
            }
        });

        streams.start();
    }
}