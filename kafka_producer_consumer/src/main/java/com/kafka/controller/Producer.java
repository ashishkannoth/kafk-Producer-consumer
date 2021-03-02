package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.model.User;

@RestController
public class Producer {

	@Autowired
	KafkaTemplate<String, User> kafkaTemplate;

	String topic = "kafka_message";

	@PostMapping("/sendMessage")
	public String sendMessage(@RequestBody User user) {
		kafkaTemplate.send(topic, user);
		return "message sent";
	}

}
