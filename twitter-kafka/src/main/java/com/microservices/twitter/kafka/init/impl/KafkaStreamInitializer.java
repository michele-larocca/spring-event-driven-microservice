package com.microservices.twitter.kafka.init.impl;

import com.microservices.config.KafkaConfigData;
import com.microservices.kafka.admin.client.KafkaAdminClient;
import com.microservices.twitter.kafka.init.StreamInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamInitializer implements StreamInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamInitializer.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaAdminClient kafkaAdminClient;

    public KafkaStreamInitializer(KafkaConfigData kafkaConfigData, KafkaAdminClient kafkaAdminClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaAdminClient = kafkaAdminClient;
    }

    @Override
    public void init() {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkSchemaRegistry();
        LOG.info("Topic with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
