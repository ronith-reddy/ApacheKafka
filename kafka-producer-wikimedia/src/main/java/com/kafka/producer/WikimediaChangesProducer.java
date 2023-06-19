package com.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static final String TOPIC_NAME = "wikimedia.recent.change";

    public static void main(String[] args) throws InterruptedException {

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        EventHandler eventHandler = new WikimediaChangesEventHandler(producer, WikimediaChangesProducer.TOPIC_NAME);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
