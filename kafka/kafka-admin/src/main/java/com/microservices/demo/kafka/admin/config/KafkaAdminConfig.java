package com.microservices.demo.kafka.admin.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.microservices.demo.app.config.KafkaConfigData;

@Configuration
public class KafkaAdminConfig {

	private final KafkaConfigData kafkaConfigData;
	
	public KafkaAdminConfig(KafkaConfigData kafkaConfigData) {
		this.kafkaConfigData = kafkaConfigData;
	}
	
	@Bean
	public AdminClient adminClient() {
		Map<String,String> map = new HashMap<>();
		map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
				kafkaConfigData.getBootstrapServers());
		return AdminClient.create(Collections.unmodifiableMap(map));		
	}
}
