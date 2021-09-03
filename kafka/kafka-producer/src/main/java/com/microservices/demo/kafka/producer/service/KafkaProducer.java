package com.microservices.demo.kafka.producer.service;

import java.io.Serializable;

import org.apache.avro.specific.SpecificRecordBase;

public interface KafkaProducer<K extends Serializable,V extends SpecificRecordBase> {
	void send(String topic,K key,V value);
}
