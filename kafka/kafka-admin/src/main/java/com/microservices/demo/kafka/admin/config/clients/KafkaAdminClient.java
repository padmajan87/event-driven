package com.microservices.demo.kafka.admin.config.clients;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import com.microservices.demo.app.config.KafkaConfigData;
import com.microservices.demo.app.config.RetryConfigData;
import com.microservices.demo.kafka.admin.config.exceptions.KafkaClientException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaAdminClient {
  private final KafkaConfigData kafkaConfigData;
  private final RetryConfigData retryConfigData;
  private final RetryTemplate retryTemplate;
  private final AdminClient adminClient;
  private final WebClient webClient;

  public KafkaAdminClient(KafkaConfigData kafkaConfigData, 
		  RetryConfigData retryConfigData, RetryTemplate retryTemplate,
		AdminClient adminClient, WebClient webClient) {
	this.kafkaConfigData = kafkaConfigData;
	this.retryConfigData = retryConfigData;
	this.retryTemplate = retryTemplate;
	this.adminClient = adminClient;
	this.webClient = webClient;
}
  
 public void createTopic() {
	 CreateTopicsResult createTopics;
	 try {
		 createTopics = retryTemplate.execute(this::doCreateTopics);
	 }
	 catch(Exception e) {
		 throw new KafkaClientException("Reached max number of retry for creating topics");
	 }
 } 
 
 public CreateTopicsResult doCreateTopics(RetryContext retryContext) {
	 List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
	 List<NewTopic> kafkaTopics = topicNames.stream().map(topic-> new NewTopic(
			 topic.trim(),
			 kafkaConfigData.getNumOfPartitions(),
			 kafkaConfigData.getReplicationFactor()
			 )).collect(Collectors.toList());
	 
	 return adminClient.createTopics(kafkaTopics);
 } 
 
 public void checkTopicsCreated() {
	 Collection<TopicListing> topics = getTopics();
	 int retryCount=1;
	 Integer maxRetry = retryConfigData.getMaxAttempts();
	 Integer multiplier = retryConfigData.getMultiplier().intValue();
	 Long sleepTimeMs = retryConfigData.getSleepTimeMs();
	 
	 for(String topic: kafkaConfigData.getTopicNamesToCreate()) {
		 while(!isTopicCreated(topics,topic)) {
			 checkMaxRetry(retryCount,maxRetry);
			 sleep(sleepTimeMs);
			 sleepTimeMs*=multiplier;
			 topics=getTopics();
		 }
	 }
	 
 }
  
 private void sleep(Long sleepTimeMs) {
	 try {
		 Thread.sleep(sleepTimeMs);
	 }
	 catch(InterruptedException e) {
		 throw new KafkaClientException("Error while sleeping while new Topics are been"
		 		+ "created");
	 }
}

private void checkMaxRetry(int retryCount, Integer maxRetry) {
	if(retryCount>maxRetry) {
		throw new KafkaClientException("Reached max number of retry for reading kafka"
				+ "topic(s)!");
	}
}

private boolean isTopicCreated(Collection<TopicListing> topics, String topic) {
   if(topic==null) {
	return false;
   }
   return topics.stream().anyMatch(t->t.name().equals(topic));
}

private Collection<TopicListing> getTopics(){
	 Collection<TopicListing> topics;
	 try {
		 topics = retryTemplate.execute(this::doGetTopicList);
	 }
	 catch(Exception e) {
		 throw new KafkaClientException("Reached max number of retry for reading topics");
	 }	 
	 return topics;
 }
 
 private Collection<TopicListing> doGetTopicList(RetryContext retryContext) 
		 throws ExecutionException,InterruptedException{
	 log.info("Reading kafka topic {} with in attempt {}",
			       kafkaConfigData.getTopicNamesToCreate().toArray(),retryContext.getRetryCount());
	 KafkaFuture<Collection<TopicListing>>  topics = adminClient.listTopics().listings();
 
	 return topics.get();
 }
 
 public void checkSchemaRegistry() {
	 int retryCount = 1;
	 Integer maxRetry= retryConfigData.getMaxAttempts();
	 int multiplier = retryConfigData.getMultiplier().intValue();
	 Long sleepTimeMs=retryConfigData.getSleepTimeMs();
	 	 
	 while(!getSchemaRegistryStatus().is2xxSuccessful()) {
		 checkMaxRetry(retryCount, maxRetry);
		 sleep(sleepTimeMs);
		 sleepTimeMs*=multiplier;
		 
	 }
 }
 
 private HttpStatus getSchemaRegistryStatus() {
	 return webClient.method(HttpMethod.GET)
	          .uri(kafkaConfigData.getSchemaRegistryUrl())
	          .exchange()
	          .map(ClientResponse::statusCode)
	          .block();
	 
 }
}
