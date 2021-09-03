package com.microservices.demo.twitter.to.kafka.service.runner.impl;


import javax.annotation.PreDestroy;

import org.springframework.stereotype.Component;

import com.microservices.demo.app.config.TwitterKeyConfig;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;

import lombok.extern.slf4j.Slf4j;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Slf4j
@Component
public class TwitterKafkaStreamRunner implements StreamRunner{

	private TwitterKeyConfig config;
	private TwitterKafkaStatusListener twitterListener;
	private TwitterStream twitterStream;
	
	public TwitterKafkaStreamRunner(TwitterKeyConfig config
			,TwitterKafkaStatusListener twitterListener) {
		this.config = config;
		this.twitterListener=twitterListener;
	}
	
	@Override
	public void start() throws TwitterException {
		twitterStream=TwitterStreamFactory.getSingleton();
		twitterStream.addListener(twitterListener);
		addFilter();
	}

	@PreDestroy
	public void shutdown() {
		if(twitterStream!=null) {
			log.info("closing twitter stream!");
			twitterStream.shutdown();
		}
	}
	
	private void addFilter() {
		String[] key = config.getTwitterKeywords();
		FilterQuery filter = new FilterQuery(key);
		twitterStream.filter(filter);
		log.info("started filtering twitter stream for keywords {}",key);
	}

}
