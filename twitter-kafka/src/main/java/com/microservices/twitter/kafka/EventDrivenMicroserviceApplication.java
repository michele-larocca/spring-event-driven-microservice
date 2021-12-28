package com.microservices.twitter.kafka;

import com.microservices.twitter.kafka.init.StreamInitializer;
import com.microservices.twitter.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices")
public class EventDrivenMicroserviceApplication implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(EventDrivenMicroserviceApplication.class);

	private final StreamRunner streamRunner;

	private final StreamInitializer streamInitializer;

	public EventDrivenMicroserviceApplication(StreamRunner streamRunner, StreamInitializer streamInitializer) {
		this.streamRunner = streamRunner;
		this.streamInitializer = streamInitializer;
	}

	public static void main(String[] args) {
		SpringApplication.run(EventDrivenMicroserviceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		LOG.info("App starts...");
		streamInitializer.init();
		streamRunner.start();
	}
}
