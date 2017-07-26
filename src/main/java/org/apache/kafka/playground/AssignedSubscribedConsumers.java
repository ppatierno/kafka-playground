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

/**
 * Created by ppatiern on 26/07/17.
 */
public class AssignedSubscribedConsumers {

    public static void main(String[] args) {

        new AssignedSubscribedConsumers().run1();
        //new AssignedSubscribedConsumers().run2();
    }

    private void run1() {

        Thread t1 = new Thread(new AssignedConsumer());
        t1.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread t2 = new Thread(new SubscribedConsumer());
        t2.start();
    }

    private void run2() {

        Thread t2 = new Thread(new SubscribedConsumer());
        t2.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread t1 = new Thread(new AssignedConsumer());
        t1.start();
    }

    private class AssignedConsumer implements Runnable {

        @Override
        public void run() {

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

            consumer.assign(Collections.singleton(new TopicPartition("test", 0)));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("AssignedConsumer: record value = " + record.value() +
                            " on topic = " + record.topic() +
                            " partition = " + record.partition());
                }
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

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

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

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("SubscribedConsumer: record value = " + record.value() +
                            " on topic = " + record.topic() +
                            " partition = " + record.partition());
                }
            }
        }
    }
}
