package com.kafka.basics.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("Starting ProducerDemoWithCallback workflow.");

        //producer config
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(producerProperties);


        for (int i = 0; i < 30; i++) {
            String key = String.valueOf(i % 3);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java_topic", key, "Hello World " + i);
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    LOGGER.info("Key - {}, partition - {}", key, recordMetadata.partition());
                    return;
                }
                LOGGER.error("Error producing: ", e);
            });

            Thread.sleep(500);
        }

        producer.flush();
        producer.close();

        LOGGER.info("Ending ProducerDemoWithCallback workflow.");
    }
}
