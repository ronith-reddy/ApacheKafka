package com.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesEventHandler implements EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesEventHandler.class);
    private KafkaProducer<String, String> kafkaProducer;
    private final String topicName;


    public WikimediaChangesEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
    }

    @Override
    public void onOpen() throws Exception {
    }

    @Override
    public void onClosed() throws Exception {
        LOGGER.info("stream closed, closing producer");
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        LOGGER.info("sending message: {}", messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topicName, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("error reading", throwable);
    }
}
