package com.microservices.demo.twitter.to.kafka.service.listener;

import org.springframework.stereotype.Component;

import com.microservices.demo.app.config.KafkaConfigData;
import com.microservices.demo.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.service.KafkaProducer;
import com.microservices.demo.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;

import lombok.extern.slf4j.Slf4j;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
@Slf4j
public class TwitterKafkaStatusListener extends StatusAdapter{

	private final KafkaConfigData kafkaConfogData;
	private final KafkaProducer<Long,TwitterAvroModel> kafkaProducer;
	private final TwitterStatusToAvroTransformer twitterStatus;
	
	public TwitterKafkaStatusListener(KafkaConfigData kafkaConfogData
			,KafkaProducer<Long,TwitterAvroModel> kafkaProducer
			,TwitterStatusToAvroTransformer twitterStatus) {
		this.kafkaConfogData =kafkaConfogData;
		this.kafkaProducer=kafkaProducer;
		this.twitterStatus = twitterStatus;
	}
	
	@Override
	public void onStatus(Status status) {
		log.info("Twitter status with text {} sent to topic {}",status.getText(),kafkaConfogData.getTopic());
		TwitterAvroModel twitterAvro = twitterStatus.getTwitterAvroModelFromStatus(status);
		System.out.println(twitterAvro.toString());
		kafkaProducer.send(kafkaConfogData.getTopic(), twitterAvro.getUserId(), twitterAvro);
	}

	
}
