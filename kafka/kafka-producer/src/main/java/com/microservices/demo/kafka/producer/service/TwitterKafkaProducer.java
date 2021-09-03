package com.microservices.demo.kafka.producer.service;

import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.microservices.demo.avro.model.TwitterAvroModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class TwitterKafkaProducer implements 
                   KafkaProducer<Long,TwitterAvroModel>{
	
	private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;
	
	public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate){
		this.kafkaTemplate=kafkaTemplate;
	}
	
	@Override
	public void send(String topic, Long key, TwitterAvroModel value) {
		log.info("sending message {} to topic {}",value,topic);
		ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture
					= kafkaTemplate.send(topic, key, value);
		kafkaResultFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
			@Override
			public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
				RecordMetadata metadata = result.getRecordMetadata();
				log.info("Recieved new metadata. Topic {}; Partition {}; Offset {}; Timestamp {} , at time {} ",
						    metadata.topic(),
						    metadata.partition(),
						    metadata.offset(),
						    metadata.timestamp(),
						    System.nanoTime());				
			}

			@Override
			public void onFailure(Throwable ex) {
				log.error("Error while sending message {} to topic {}",
						value.toString(),topic,ex);
			}
		
		});
	}
	
	@PreDestroy
	public void close() {
		if(kafkaTemplate!=null) {
			log.info("Closing kafka producer!");
			kafkaTemplate.destroy();
		}
	}
}
