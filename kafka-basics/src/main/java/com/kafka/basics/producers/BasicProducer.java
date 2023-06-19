package com.kafka.basics.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BasicProducer {

    public static final Logger LOGGER = LoggerFactory.getLogger(BasicProducer.class);

    public static void main(String[] args) {
        LOGGER.info("Starting BasicProducer workflow.");

        //producer config
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(producerProperties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java_topic", "Hello World!");

        producer.send(producerRecord);

        producer.flush();
        producer.close();
    }
}
