package org.apache.kafka.playground;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ppatiern on 26/07/17.
 */
public class AssignedSubscribedConsumers {

    private volatile boolean consuming = true;
    private ExecutorService executorService;

    public void start() {

        this.executorService = Executors.newFixedThreadPool(2);
        this.executorService.submit(new AssignedConsumer());
        this.executorService.submit(new SubscribedConsumer());
    }

    public void stop() throws InterruptedException {

        this.consuming = false;
        this.executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        this.executorService.shutdownNow();
    }

    public static void main(String[] args) throws Exception {

        AssignedSubscribedConsumers consumers = new AssignedSubscribedConsumers();
        consumers.start();
        System.in.read();
        consumers.stop();
    }

    private class AssignedConsumer implements Runnable {

        @Override
        public void run() {

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = null;

            try {
                consumer = new KafkaConsumer<>(props);
                consumer.assign(Collections.singleton(new TopicPartition("test", 0)));

                while (consuming) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("AssignedConsumer: record value = " + record.value() +
                                " on topic = " + record.topic() +
                                " partition = " + record.partition());
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
    }

    private class SubscribedConsumer implements Runnable {

        @Override
        public void run() {

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = null;
            try {

                consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singleton("test"), new ConsumerRebalanceListener() {

                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        if (collection.size() > 0) {
                            System.out.println("Revoked partitions ...");
                            for (TopicPartition topicPartition : collection) {
                                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                            }
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                        if (collection.size() > 0) {
                            System.out.println("Assigned partitions ...");
                            for (TopicPartition topicPartition : collection) {
                                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                            }
                        }
                    }
                });

                while (consuming) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("SubscribedConsumer: record value = " + record.value() +
                                " on topic = " + record.topic() +
                                " partition = " + record.partition());
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
    }
}
