package com.kafka.controller;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {

	@KafkaListener(topics="kafka_message", groupId ="group_id")
	public void consumedMessage(String message) {
		System.out.println("Ther message is = "+ message);
	}
}
