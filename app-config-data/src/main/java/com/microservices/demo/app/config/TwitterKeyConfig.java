package com.microservices.demo.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix="twitter-to-kafka-service")
public class TwitterKeyConfig {
  private String[] twitterKeywords;
}
