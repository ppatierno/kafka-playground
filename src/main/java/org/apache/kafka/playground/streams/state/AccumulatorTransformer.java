package org.apache.kafka.playground.streams.state;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * AccumulatorTransformer
 */
public class AccumulatorTransformer implements FixedKeyProcessor<String, Integer, Integer> {

    private KeyValueStore<String, Integer> stateStore;
    private FixedKeyProcessorContext<String, Integer> context;

    @Override
    public void init(FixedKeyProcessorContext<String, Integer> context) {
        this.context = context;
        this.stateStore = context.getStateStore("accumulatorStore");
    }

    @Override
    public void process(FixedKeyRecord<String, Integer> record) {
        Integer accumulator = record.value();
        Integer accumulatedSoFar = this.stateStore.get(record.key());
        if (accumulatedSoFar != null) {
            accumulator += accumulatedSoFar;
        }
        this.stateStore.put(record.key(), accumulator);
        this.context.forward(record.withValue(accumulator));
    }

    @Override
    public void close() {
        // no operation
    }



}