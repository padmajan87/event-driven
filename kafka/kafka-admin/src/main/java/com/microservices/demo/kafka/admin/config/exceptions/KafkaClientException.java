package com.microservices.demo.kafka.admin.config.exceptions;


public class KafkaClientException extends RuntimeException {

	public KafkaClientException(String message) {
		super(message);
	}
}
