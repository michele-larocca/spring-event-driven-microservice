package com.microservices.twitter.kafka;

import com.microservices.config.TwitterKafkaConfiguration;
import com.microservices.twitter.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices")
public class EventDrivenMicroserviceApplication implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(EventDrivenMicroserviceApplication.class);

	private final TwitterKafkaConfiguration twitterKafkaConf;
	private final StreamRunner streamRunner;

	public EventDrivenMicroserviceApplication(TwitterKafkaConfiguration conf, StreamRunner streamRunner) {
		this.twitterKafkaConf = conf;
		this.streamRunner = streamRunner;
	}

	public static void main(String[] args) {
		SpringApplication.run(EventDrivenMicroserviceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("App initialized !!!");
		LOG.info(Arrays.toString(twitterKafkaConf.getTwitterKeywords().toArray()));
		streamRunner.start();
	}
}
