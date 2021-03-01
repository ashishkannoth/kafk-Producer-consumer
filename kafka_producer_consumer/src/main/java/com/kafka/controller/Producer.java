package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Producer {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	String topic = "kafka_message";

	@GetMapping("/sendMessage")
	public String sendMessage(@RequestParam String message) {
		kafkaTemplate.send(topic, message);
		return "message sent";
	}

}
