package com.microservices.twitter.kafka.runner.impl;

import com.microservices.config.TwitterKafkaConfiguration;
import com.microservices.twitter.kafka.exception.TwitterKafkaServiceException;
import com.microservices.twitter.kafka.listener.TwitterKafkaStatusListener;
import com.microservices.twitter.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name="twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterKafkaConfiguration twitterKafkaConfiguration;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[] {
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetur",
            "adipiscing",
            "elit",
            "Sed",
            "vitae",
            "tellus",
            "in",
            "turpis",
            "rhoncus",
            "viverra",
            "enean",
            "id",
            "leo",
            "malesuada",
            "dapibus",
            "lectus",
            "in",
            "efficitur",
            "eros"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterKafkaConfiguration conf, TwitterKafkaStatusListener status) {
        twitterKafkaConfiguration = conf;
        twitterKafkaStatusListener = status;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterKafkaConfiguration.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterKafkaConfiguration.getMockMinTweetLength();
        int maxTweetLength = twitterKafkaConfiguration.getMockMaxTweetLength();
        long sleepTime =  twitterKafkaConfiguration.getMockSleepMs();
        LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTime);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTime) throws TwitterException {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTime);
                }
            }catch (TwitterException e){
                LOG.error("Error creating twitter simulatade stream", e);
            }
        });
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new TwitterKafkaServiceException("Error while sleeping for waiting new status to create!", e);
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[] {
            ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT)),
            String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
            getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
            String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };

        return formatTweetAsJsonWithParam(params);
    }

    private String formatTweetAsJsonWithParam(String[] params) {
        String tweet = tweetAsRawJson;

        for(int i = 0; i < params.length; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength / 2)
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
        }
        return tweet.toString().trim();
    }
}
