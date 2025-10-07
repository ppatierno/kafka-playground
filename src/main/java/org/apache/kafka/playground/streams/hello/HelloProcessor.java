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

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloProcessor implements Processor<String, String, String, String> {

    private static final Logger log = LoggerFactory.getLogger(HelloProcessor.class);

    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        log.info("init");
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        log.info("process");
        this.context.forward(new Record<>(record.key(), String.format("Hello %s !", record.value()), record.timestamp()));
    }

    @Override
    public void close() {
        log.info("close");
    }
}
