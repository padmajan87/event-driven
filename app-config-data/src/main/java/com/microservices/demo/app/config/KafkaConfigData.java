package com.microservices.demo.app.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix="kafka-config")
public class KafkaConfigData {
	private String bootstrapServers;
	private String schemaRegistryUrlKey;
	private String schemaRegistryUrl;
	private String topic;
	private List<String> topicNamesToCreate;
	private Integer numOfPartitions;
	private short replicationFactor;
}
