package com.microservices.demo.twitter.to.kafka.service.init.impl;

import org.springframework.stereotype.Service;

import com.microservices.demo.app.config.KafkaConfigData;
import com.microservices.demo.kafka.admin.config.clients.KafkaAdminClient;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaStreamInitializer implements StreamInitializer{

	private final KafkaConfigData kafkaConfigData;
	private final KafkaAdminClient kafkaAdminClient;
	
	public KafkaStreamInitializer(KafkaConfigData kafkaConfigData
			,KafkaAdminClient kafkaAdminClient) {
		this.kafkaConfigData = kafkaConfigData;
		this.kafkaAdminClient = kafkaAdminClient;
	}

	@Override
	public void init() {
		kafkaAdminClient.createTopic();
		kafkaAdminClient.checkSchemaRegistry();
		log.info("Topic for name {} is ready for operations "
				,kafkaConfigData.getTopicNamesToCreate().toArray());
	}

}
