package org.apache.kafka.playground;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class AddRemoveSubscriptions implements ConsumerRebalanceListener {

    private List<String> topics;

    public static void main(String[] args) {

        new AddRemoveSubscriptions().run();
    }

    private void run() {

        topics = new ArrayList<>();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = null;

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("topics"), this);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Consumer: record value = " + record.value() +
                        " on topic = " + record.topic() +
                        " partition = " + record.partition());

                if (record.value().startsWith("add:")) {
                    String topic = record.value().substring("add:".length());
                    // avoiding operation on "topics" and adding an already subscribed topic
                    if (!topic.equals("topics") && !topics.contains(topic)) {
                        List<String> topics = new ArrayList<>(this.topics);
                        topics.add(topic);
                        consumer.subscribe(topics, this);
                    }
                } else if (record.value().startsWith("remove:")) {
                    String topic = record.value().substring("remove:".length());
                    // avoiding operation on "topics" and removing a not subscribed topic
                    if (!topic.equals("topics") && topics.contains(topic)) {
                        List<String> topics = new ArrayList<>(this.topics);
                        topics.remove(topic);
                        consumer.subscribe(topics, this);
                    }
                }
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        if (collection.size() > 0) {
            System.out.println("Revoked partitions ...");
            for (TopicPartition topicPartition : collection) {
                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                topics.remove(topicPartition.topic());
            }
            System.out.println("topics = " + topics);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        if (collection.size() > 0) {
            System.out.println("Assigned partitions ...");
            for (TopicPartition topicPartition : collection) {
                System.out.println("topic=" + topicPartition.topic() + " partition=" + topicPartition.partition());
                topics.add(topicPartition.topic());
            }
            System.out.println("topics = " + topics);
        }
    }

}
