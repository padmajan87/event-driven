package com.microservices.demo.kafka.producer.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.microservices.demo.app.config.KafkaConfigData;
import com.microservices.demo.app.config.KafkaProducerConfigData;

@Configuration
public class KafkaProducerConfig<K extends Serializable,V extends SpecificRecordBase> {

	private final KafkaConfigData kafkaConfigData;
	private final KafkaProducerConfigData kafkaProdConfigData;
	
	public KafkaProducerConfig(KafkaConfigData kafkaConfigData,
			KafkaProducerConfigData kafkaProdConfigData) {
		super();
		this.kafkaConfigData = kafkaConfigData;
		this.kafkaProdConfigData = kafkaProdConfigData;
	}
	
	@Bean
	public Map<String,Object> producerConfig() {
		System.out.println(kafkaProdConfigData.getBatchSize());
		Map<String,Object> props= new HashMap();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
				, kafkaConfigData.getBootstrapServers());
		props.put(kafkaConfigData.getSchemaRegistryUrlKey()
				,kafkaConfigData.getSchemaRegistryUrl());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
				,kafkaProdConfigData.getKeySerializerClass());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
				, kafkaProdConfigData.getValueSerializerClass());
		props.put(ProducerConfig.BATCH_SIZE_CONFIG,
				kafkaProdConfigData.getBatchSize()
				+ kafkaProdConfigData.getBatchSizeBoostFactor());
		props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProdConfigData.getLingerMs());
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG
				,kafkaProdConfigData.getCompressionType() );
		props.put(ProducerConfig.ACKS_CONFIG, kafkaProdConfigData.getAcks());
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG
				, kafkaProdConfigData.getRequestTimeoutMs());
		props.put(ProducerConfig.RETRIES_CONFIG,kafkaProdConfigData.getRetryCount());
		
		return props;
	}
	
	@Bean
	public ProducerFactory<K,V> producerFactory(){
		return new DefaultKafkaProducerFactory<>(producerConfig());
	}
	
	@Bean
	public KafkaTemplate<K, V> kafkaTemplate(){
		return new KafkaTemplate<>(producerFactory());
	}
	
}
