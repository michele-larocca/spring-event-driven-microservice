package com.microservices.twitter.kafka.runner.impl;

import com.microservices.config.TwitterKafkaConfiguration;
import com.microservices.twitter.kafka.listener.TwitterKafkaStatusListener;
import com.microservices.twitter.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.*;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name="twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterKafkaConfiguration twitterKafkaConfiguration;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterKafkaConfiguration configuration,
                                    TwitterKafkaStatusListener statusListener) {
        this.twitterKafkaConfiguration = configuration;
        this.twitterKafkaStatusListener = statusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream  = TwitterStreamFactory.getSingleton();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown(){
        if(twitterStream != null) {
            LOG.info("Closing twitter stream!");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keys = twitterKafkaConfiguration.getTwitterKeywords().toArray(new String[0]);
        twitterStream.filter(new FilterQuery(keys));
        LOG.info("Started Filtering twitter stream by keywords ", Arrays.toString(keys));
    }
}
