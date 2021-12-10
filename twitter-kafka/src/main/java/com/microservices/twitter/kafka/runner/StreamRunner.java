package com.microservices.twitter.kafka.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
    public void start() throws TwitterException;
}
