package org.apache.kafka.playground.streams.state;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * AccumulatorTransformer
 */
public class AccumulatorTransformer implements ValueTransformerWithKey<String, Integer, Integer> {

    private KeyValueStore<String, Integer> stateStore;

    @Override
    public void init(ProcessorContext context) {
        this.stateStore = (KeyValueStore) context.getStateStore("accumulatorStore");

    }

    @Override
    public Integer transform(String readOnlyKey, Integer value) {
        Integer accumulator = value;
        Integer accumulatedSoFar = this.stateStore.get(readOnlyKey);
        if (accumulatedSoFar != null) {
            accumulator += accumulatedSoFar;
        }
        this.stateStore.put(readOnlyKey, accumulator);
        return accumulator;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

   
    
}