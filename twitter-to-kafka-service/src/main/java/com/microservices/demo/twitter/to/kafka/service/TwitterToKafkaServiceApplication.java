package com.microservices.demo.twitter.to.kafka.service;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.microservices.demo.app.config.TwitterKeyConfig;
import com.microservices.demo.twitter.to.kafka.service.init.impl.StreamInitializer;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner{

	private final StreamRunner streamRunner;
	private final StreamInitializer streamInit;
	
	public TwitterToKafkaServiceApplication(
			StreamRunner streamRunner, StreamInitializer streamInit) {
		this.streamRunner=streamRunner;
		this.streamInit = streamInit;
	}
	
	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("App Starts ");
		streamInit.init();
		streamRunner.start();
	}
}
