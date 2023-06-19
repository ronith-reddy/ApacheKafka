package com.kafka.basics.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        LOGGER.info("Starting BasicProducer workflow.");

        String groupId = "java-demo-application";

        Properties properties = getProperties(groupId);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        addShutdownHook(kafkaConsumer);
        kafkaConsumer.subscribe(Arrays.asList("demo_java_topic"));

        try {
            consumeRecords(kafkaConsumer);
        } catch (WakeupException ex) {
            LOGGER.info("Consumer is starting to shutdown");
        } catch (Exception ex) {
            LOGGER.error("unhandled exception", ex);
        } finally {
            kafkaConsumer.close();
        }
    }

    private static void consumeRecords(KafkaConsumer<String, String> kafkaConsumer) {
        while (true) {
            LOGGER.info("Polling");

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                LOGGER.info("key: {}, value: {}", consumerRecord.key(), consumerRecord.value());
                LOGGER.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
            }
        }
    }

    private static Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

    private static void addShutdownHook(KafkaConsumer<String, String> kafkaConsumer) {
        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
