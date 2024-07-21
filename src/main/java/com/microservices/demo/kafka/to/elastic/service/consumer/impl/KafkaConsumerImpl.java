package com.microservices.demo.kafka.to.elastic.service.consumer.impl;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.admin.client.KafkaAdminClient;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.to.elastic.service.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.List;
import java.util.Objects;

@Service
public class KafkaConsumerImpl implements KafkaConsumer<Long, TwitterAvroModel> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaConfigData kafkaConfigData;

    public KafkaConsumerImpl(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                             KafkaAdminClient kafkaAdminClient, KafkaConfigData kafkaConfigData) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
    }

    @EventListener
    //EventListener is one of the ways of running a code on application start
    public void onAppStarted(ApplicationStartedEvent startedEvent) {
        kafkaAdminClient.checkTopicCreated();
        LOGGER.info("Topics with names {} are ready for operation", kafkaConfigData.getTopicNamesToCreate());
        Objects.requireNonNull(kafkaListenerEndpointRegistry.getListenerContainer("twitterTopicListener")).start();

    }

    @Override
    @KafkaListener(id = "twitterTopicListener", topics = "${kafka-config.topic-name}")
    public void receive(@Payload List<TwitterAvroModel> messages,
                        @RequestHeader(KafkaHeaders.RECEIVED_KEY) List<Integer> keys,
                        @RequestHeader(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partition,
                        @RequestHeader(KafkaHeaders.OFFSET) List<Integer> offsets) {
        LOGGER.info("Topic: {}, {} number of messages received with keys: {}, partiotions: {} and offsets: {}," +
                "sending it to elastic, Thread id: {}",
                kafkaConfigData.getTopicName(),
                messages.size(),
                keys,
                partition,
                offsets,
                Thread.currentThread().getName()
        );
    }
}
